from rq import Worker

from queue_config import get_queue, get_redis_connection

if __name__ == '__main__':
    queue = get_queue()
    worker = Worker([queue], connection=get_redis_connection())
    worker.work()
