import os
import time
import logging
from kafka import KafkaProducer
from json import dumps

KSERVE_API_DEFAULT_KAFKA_ENDPOINT = os.environ.get('KSERVE_API_DEFAULT_KAFKA_ENDPOINT')

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]: {} %(levelname)s %(message)s'.format(os.getpid()),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger()


def makeproducer():
    try:
        producer = KafkaProducer(
            acks=1,
            compression_type='gzip',
            bootstrap_servers=[KSERVE_API_DEFAULT_KAFKA_ENDPOINT],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        return producer
    except Exception as err:
        logger.exception(err)
        time.sleep(30)
        makeproducer()


def produceKafka(producer, message, kind):
    producer.send(f'{kind}_monitor_list', value=message)
    producer.flush()
