#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
import signal
from typing import Optional

from pendulum import utcfromtimestamp
from sqlalchemy import or_

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job import BaseJob
from airflow.models.taskinstance import TaskInstance
from airflow.stats import Stats
from airflow.task.task_runner import get_task_runner
from airflow.utils import timezone
from airflow.utils.dag_processing import SimpleTaskInstance
from airflow.utils.helpers import create_redis_connection
from airflow.utils.net import get_hostname
from airflow.utils.session import provide_session
from airflow.utils.state import State


class LocalTaskJob(BaseJob):
    """
    LocalTaskJob runs a single task instance.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    redis_batch_size = conf.getint('heartbeat', 'redis_get_batch_size')

    def __init__(
            self,
            task_instance: TaskInstance,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            pickle_id: Optional[str] = None,
            pool: Optional[str] = None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        self.task_runner = None

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False

        super().__init__(*args, **kwargs)

    def _execute(self):
        self.task_runner = get_task_runner(self)

        # pylint: disable=unused-argument
        def signal_handler(signum, frame):
            """Setting kill signal handler"""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.on_kill()
            raise AirflowException("LocalTaskJob received SIGTERM signal")

        # pylint: enable=unused-argument
        signal.signal(signal.SIGTERM, signal_handler)

        if not self.task_instance.check_and_change_state_before_execution(
                mark_success=self.mark_success,
                ignore_all_deps=self.ignore_all_deps,
                ignore_depends_on_past=self.ignore_depends_on_past,
                ignore_task_deps=self.ignore_task_deps,
                ignore_ti_state=self.ignore_ti_state,
                job_id=self.id,
                pool=self.pool):
            self.log.info("Task is not able to be run")
            return

        try:
            self.task_runner.start()

            heartbeat_time_limit = conf.getint('scheduler',
                                               'scheduler_zombie_task_threshold')
            while True:
                # Monitor the task to see if it's done
                return_code = self.task_runner.return_code()
                if return_code is not None:
                    self.log.info("Task exited with return code %s", return_code)
                    return

                self.heartbeat()

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                # This can only really happen if the worker can't read the DB for a long time
                time_since_last_heartbeat = (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.log.error("Heartbeat time limit exceeded!")
                    raise AirflowException("Time since last heartbeat({:.2f}s) "
                                           "exceeded limit ({}s)."
                                           .format(time_since_last_heartbeat,
                                                   heartbeat_time_limit))
        finally:
            self.on_kill()

    def on_kill(self):
        self.task_runner.terminate()
        self.task_runner.on_finish()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # ensure termination if processes are created later
            self.task_runner.terminate()
            return

        self.task_instance.refresh_from_db()
        ti = self.task_instance

        if ti.state == State.RUNNING:
            fqdn = get_hostname()
            same_hostname = fqdn == ti.hostname
            if not same_hostname:
                self.log.warning("The recorded hostname %s "
                                 "does not match this instance's hostname "
                                 "%s", ti.hostname, fqdn)
                raise AirflowException("Hostname of job runner does not match")

            current_pid = os.getpid()
            same_process = ti.pid == current_pid
            if not same_process:
                self.log.warning("Recorded pid %s does not match "
                                 "the current pid %s", ti.pid, current_pid)
                raise AirflowException("PID of job runner does not match")
        elif (
                self.task_runner.return_code() is None and
                hasattr(self.task_runner, 'process')
        ):
            self.log.warning(
                "State of this instance has been externally set to %s. "
                "Terminating instance.",
                ti.state
            )
            if ti.state == State.FAILED and ti.task.on_failure_callback:
                context = ti.get_template_context()
                ti.task.on_failure_callback(context)
            if ti.state == State.SUCCESS and ti.task.on_success_callback:
                context = ti.get_template_context()
                ti.task.on_success_callback(context)
            self.task_runner.terminate()
            self.terminating = True


    @classmethod
    def get_zombie_running_tis(cls, limit_dttm, logger, session=None):
        TI = TaskInstance

        if cls.redis_enabled:
            with create_redis_connection(conf.get('heartbeat', 'redis_url')) as redis_client:
                def batch_get(items):
                    # Redis has no batch functionality for zscore
                    # see https://github.com/antirez/redis/issues/2344
                    lua_script = '''
                        local res = {}
                        while #ARGV > 0 do
                            res[#res+1] = redis.call('ZSCORE', KEYS[1], table.remove(ARGV, 1))
                        end
                        return res
                    '''
                    from airflow.jobs.local_task_job import LocalTaskJob
                    batch_starts_at = time.time()

                    res = redis_client.register_script(lua_script)(keys=[LocalTaskJob.__name__], args=items)

                    logger.info('Loading %s heartbeats, time: %s', len(res), time.time() - batch_starts_at)

                    return res

                ti_job = session.query(TI, cls).join(cls, TI.job_id==cls.id).filter(TI.state==State.RUNNING).all()

                job_id_to_ti_job = dict([(job.id, (ti, job)) for ti, job in ti_job])
                job_ids = list(job_id_to_ti_job.keys())

                zombie_tis = []
                for idx in range(0, len(job_ids), cls.redis_batch_size):
                    try:
                        batch_job_ids = job_ids[idx:idx+cls.redis_batch_size]
                        heartbeats = batch_get(batch_job_ids)

                        for heartbeat_epoch, job_id in zip(heartbeats, batch_job_ids):
                            ti, job = job_id_to_ti_job[job_id]

                            heartbeat = heartbeat_epoch and utcfromtimestamp(float(heartbeat_epoch))
                            # fallback to job._orm_heartbeat so that
                            # during rolling out, it will not cause zombies as there are long running tasks
                            if ((not heartbeat and job._orm_heartbeat < limit_dttm) or
                                    (heartbeat and heartbeat < limit_dttm) or
                                    job.state != State.RUNNING):
                                sti = SimpleTaskInstance(ti)
                                logger.info(
                                    "Detected zombie job_id %s with dag_id %s, task_id %s, and execution date %s and heartbeat %s",
                                    job_id, sti.dag_id, sti.task_id, sti.execution_date.isoformat(), heartbeat)
                                zombie_tis.append(sti)
                    except Exception as e:
                        logger.exception('Exception happened while handling batch %s, but still keep processing rest batch', idx)

                return zombie_tis
        else:
            tis = (
                session
                .query(TI)
                .join(cls, TI.job_id == cls.id)
                .filter(TI.state == State.RUNNING)
                .filter(
                    or_(
                        cls.state != State.RUNNING,
                        cls._orm_heartbeat < limit_dttm,
                    )
                ).all()
            )
            zombies = [SimpleTaskInstance(ti) for ti in tis]
            for sti in zombies:
                logger.info(
                    "Detected zombie job with dag_id %s, task_id %s, and execution date %s",
                    sti.dag_id, sti.task_id, sti.execution_date.isoformat())
            return zombies
