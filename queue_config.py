import os
from redis import Redis
from rq import Queue

DEFAULT_REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
JOB_QUEUE_NAME = os.getenv('RQ_QUEUE_NAME', 'provider-crawls')


def get_redis_connection():
    return Redis.from_url(DEFAULT_REDIS_URL)


def get_queue():
    return Queue(JOB_QUEUE_NAME, connection=get_redis_connection(), default_timeout=60 * 60)
