import uuid
import json
import time
import sys
import redis
import signal
from loguru import logger
from multiprocessing import Process


class Pub:
    def __init__(self, channel: str) -> None:
        self.loop = True
        self.channel = channel
    
    def run_pub(self):
        logger.info(
            f'[main][pub][{self.channel}] starting new publisher'
        )
        while self.loop:
            message = {
                'uuid': str(uuid.uuid4()),
                'message': 'hello'
            }
            redis_client.publish(self.channel, json.dumps(message))
            logger.info(
                f'[main][pub][{self.channel}] sending message: {message}'
            )
            time.sleep(10)
    
    def stop_pub(self):
        message = {
            'uuid': str(uuid.uuid4()),
            'message': 'hello'
        }
        logger.warning(
            '[pub] Shutting pub application and publish last message on channel'
        )
        redis_client.publish(self.channel, json.dumps(message))
        logger.info(
            f'[main][pub][{self.channel}] sending message: {message}'
        )


class Sub:
    def __init__(self, channel, subscriber) -> None:
        self.loop = True
        self.channel = channel
        self.subscriber = subscriber
    
    def run_sub(self):
        logger.info(
            f'[main][sub][{self.channel}] starting new subscriber'
        )

        while self.loop:
            message = self.subscriber.get_message()
            logger.info(
                f'[main][sub][{self.channel}] message: {message}'
            )
            time.sleep(60)
    
    def stop_sub(self):
        logger.warning(
            '[sub] Shutting sub application and getting all messages on channel'
        )
        while True:
            message = self.subscriber.get_message()
            if not message:
                break

            logger.info(
                f'[main][sub][{self.channel}] message: {message}'
            )
    

if __name__ == '__main__':
    try:
        redis_client = redis.StrictRedis(host="localhost", port=6379)
        channel = f'channel-{str(uuid.uuid4())}'
        subscriber = redis_client.pubsub()
        subscriber.psubscribe(channel)
        sub_obj = Sub(channel=channel, subscriber=subscriber)
        pub_obj = Pub(channel=channel)

        def handle_signal(signum, frame):
            logger.warning(f"[{channel}] Received signal {signum}, shutting down gracefully...")
            pub_obj.stop_pub()
            sub_obj.stop_sub()
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
        sub = Process(target=sub_obj.run_sub)
        pub = Process(target=pub_obj.run_pub)
        sub.start()
        pub.start()
    
    except Exception as error:
        logger.error(
          f'[main] raised the follwing error {error}'
        )
        sub.join()
        pub.join()
    
    finally:
        sub.join()
        pub.join()
