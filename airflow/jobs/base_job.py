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

import getpass
from time import sleep
from typing import Optional

from pendulum import utcfromtimestamp
from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.session import make_transient

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models.base import ID_LEN, Base
from airflow.models.taskinstance import TaskInstance
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.helpers import convert_camel_to_snake, create_redis_connection
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session, provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State


class BaseJob(Base, LoggingMixin):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have its own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))
    """
    because this can be backed by sqlalchemy or redis, name is underscored so
    users can use get_heartbeat and update_heartbeat
    """
    _orm_heartbeat = Column(UtcDateTime())

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, _orm_heartbeat),
        Index('idx_job_state_heartbeat', state, _orm_heartbeat),
    )

    task_instances_enqueued = relationship(
        TaskInstance,
        primaryjoin=id == TaskInstance.queued_by_job_id,
        foreign_keys=id,
        backref=backref('queued_by_job', uselist=False),
    )
    """
    TaskInstances which have been enqueued by this Job.

    Only makes sense for SchedulerJob and BackfillJob instances.
    """

    heartrate = conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC')
    redis_enabled = conf.getboolean('heartbeat', 'redis_enabled')

    def __init__(
            self,
            executor=None,
            heartrate=None,
            *args, **kwargs):
        self.hostname = get_hostname()
        self.executor = executor or ExecutorLoader.get_default_executor()
        self.executor_class = self.executor.__class__.__name__
        self.start_date = timezone.utcnow()
        self._orm_heartbeat = timezone.utcnow()
        if heartrate is not None:
            self.heartrate = heartrate
        self.unixname = getpass.getuser()
        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        super().__init__(*args, **kwargs)

    @classmethod
    @provide_session
    def most_recent_job(cls, session=None) -> Optional['BaseJob']:
        """
        Return the most recent job of this type, if any, based on last
        heartbeat received.

        This method should be called on a subclass (i.e. on SchedulerJob) to
        return jobs of that type.

        :param session: Database session
        :rtype: BaseJob or None
        """
        latest_job_sql = session.query(cls).order_by(cls._orm_heartbeat.desc()).limit(1).first()
        if cls.redis_enabled:
            with create_redis_connection(conf.get('heartbeat', 'redis_url')) as redis_client:
                latest_id = redis_client.zrange(cls.__name__, -1, -1)
                latest_job_redis = latest_id and session.query(cls).get(int(latest_id))

                return latest_job_redis
        else:
            return latest_job_sql

    def is_alive(self, grace_multiplier=2.1):
        """
        Is this job currently alive.

        We define alive as in a state of RUNNING, and having sent a heartbeat
        within a multiple of the heartrate (default of 2.1)

        :param grace_multiplier: multiplier of heartrate to require heart beat
            within
        :type grace_multiplier: number
        :rtype: boolean
        """
        return (
            self.state == State.RUNNING and
            (timezone.utcnow() - self.latest_heartbeat).total_seconds() < self.heartrate * grace_multiplier
        )

    @provide_session
    def kill(self, session=None):
        """
        Handles on_kill callback and updates state in database.
        """
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = timezone.utcnow()
        try:
            self.on_kill()
        except Exception as e:  # pylint: disable=broad-except
            self.log.error('on_kill() method failed: %s', str(e))
        session.merge(job)
        session.commit()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        """
        Will be called when an external kill command is received
        """

    def heartbeat_callback(self, session=None):
        """
        Callback that is called during heartbeat. This method should be overwritten.
        """

    def heartbeat(self, only_if_necessary: bool = False):
        """
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heart rate is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.

        :param only_if_necessary: If the heartbeat is not yet due then do
            nothing (don't update column, don't call ``heartbeat_callback``)
        :type only_if_necessary: boolean
        """
        seconds_remaining = 0
        if self.latest_heartbeat:
            seconds_remaining = self.heartrate - (timezone.utcnow() - self.latest_heartbeat).total_seconds()

        if seconds_remaining > 0 and only_if_necessary:
            return

        if self.redis_enabled:
            import redis
            HeartbeatExceptionToCatch = (redis.RedisError, OperationalError) # parent class of all redis exceptions
        else:
            HeartbeatExceptionToCatch = OperationalError

        try:
            with create_session() as session:
                # This will cause it to load from the db
                session.merge(self)

            if self.state == State.SHUTDOWN:
                self.kill()

            # Figure out how long to sleep for
            sleep_for = 0
            if self.latest_heartbeat:
                seconds_remaining = self.heartrate - \
                    (timezone.utcnow() - self.latest_heartbeat)\
                    .total_seconds()
                sleep_for = max(0, seconds_remaining)
            sleep(sleep_for)

            self.update_heartbeat()

            self.heartbeat_callback(session=session)
            self.log.debug('[heartbeat]')
        except HeartbeatExceptionToCatch:
            Stats.incr(
                convert_camel_to_snake(self.__class__.__name__) + '_heartbeat_failure', 1,
                1)
            self.log.exception("%s heartbeat got an exception", self.__class__.__name__)

    def update_heartbeat(self, heartbeat_time=None):
        if heartbeat_time is None:
            heartbeat_time = timezone.utcnow()

        self.logger.info('Updating heartbeat: %s', heartbeat_time)

        if self.redis_enabled:
            with create_redis_connection(conf.get('heartbeat', 'redis_url')) as redis_client:
                redis_client.zadd(
                    self.job_type,
                    {str(self.id): str((heartbeat_time - timezone.utc_epoch()).total_seconds())})
        else:
            with create_session() as session:
                self._orm_heartbeat = heartbeat_time

                session.merge(self)
                session.commit()

    @property
    def latest_heartbeat(self):
        if self.redis_enabled:
            with create_redis_connection(conf.get('heartbeat', 'redis_url')) as redis_client:
                redis_result = redis_client.zscore(self.job_type, str(self.id))
                # fallback to self._orm_heartbeat so that
                # during rolling out, it will not cause zombies as there are long running tasks
                return (redis_result and utcfromtimestamp(redis_result)) or self._orm_heartbeat
        else:
            return self._orm_heartbeat

    def run(self):
        """
        Starts the job.
        """
        Stats.incr(self.__class__.__name__.lower() + '_start', 1, 1)
        # Adding an entry in the DB
        with create_session() as session:
            self.state = State.RUNNING
            session.add(self)
            session.commit()
            make_transient(self)

            # heartbeat when the id is available
            self.update_heartbeat()

            try:
                self._execute()
                # In case of max runs or max duration
                self.state = State.SUCCESS
            except SystemExit:
                # In case of ^C or SIGTERM
                self.state = State.SUCCESS
            except Exception:
                self.state = State.FAILED
                raise
            finally:
                self.end_date = timezone.utcnow()
                session.merge(self)
                session.commit()

        Stats.incr(self.__class__.__name__.lower() + '_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("This method needs to be overridden")
