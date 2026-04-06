import logging
import sys
import time

from rq import Worker

from queue_config import get_queue, get_redis_connection

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    for attempt in range(1, 6):
        try:
            conn = get_redis_connection()
            conn.ping()
            logger.info('Redis connection OK')
            queue = get_queue()
            worker = Worker([queue], connection=conn)
            logger.info('Worker starting, listening on queue: %s', queue.name)
            worker.work()
            break
        except Exception as exc:
            logger.error('Worker startup error (attempt %d/5): %s', attempt, exc)
            if attempt == 5:
                sys.exit(1)
            time.sleep(attempt * 2)
