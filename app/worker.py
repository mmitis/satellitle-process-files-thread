from redis import Redis
from rq import Worker, Queue

from .config import settings

if __name__ == "__main__":
    redis_conn = Redis.from_url(settings.redis_url)
    queues = [Queue("thread", connection=redis_conn)]
    worker = Worker(queues, connection=redis_conn)
    worker.work()
