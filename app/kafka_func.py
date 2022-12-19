import os
import time
import logging
from kafka import KafkaProducer
from kafka.admin import new_topic, new_partitions, KafkaAdminClient
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


def initTopic():
    client = KafkaAdminClient(bootstrap_servers=[KSERVE_API_DEFAULT_KAFKA_ENDPOINT])
    create_list = []

    topics = client.list_topics()
    if 'drift_monitor_list' not in topics:
        create_list.append('drift_monitor_list')
    if 'accuracy_monitor_list' not in topics:
        create_list.append('accuracy_monitor_list')
    if 'service_monitor_list' not in topics:
        create_list.append('service_monitor_list')
    if len(create_list) != 0:
        create_topic = []
        for i in create_list:
            create_topic.append(new_topic.NewTopic(name=i, num_partitions=10, replication_factor=1))
        client.create_topics(new_topics=create_topic)
    des_to = client.describe_topics(topics=['drift_monitor_list', 'accuracy_monitor_list', 'service_monitor_list'])
    update_list = []
    for i in des_to:
        if 10 > len(i['partitions']):
            client.create_partitions({
                i['topic']: new_partitions.NewPartitions(10)
            })
            update_list.append(i['topic'])

    print(f"create: {create_list}, update: {update_list}")
