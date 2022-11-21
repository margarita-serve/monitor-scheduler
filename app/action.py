import logging
import os
import time

from elasticsearch import Elasticsearch, exceptions
from kafka_func import produceKafka, makeproducer

KSERVE_API_DEFAULT_DATABASE_ENDPOINT = os.environ.get('KSERVE_API_DEFAULT_DATABASE_ENDPOINT')

ES = Elasticsearch(KSERVE_API_DEFAULT_DATABASE_ENDPOINT)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]: {} %(levelname)s %(message)s'.format(os.getpid()),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger()


def drift_monitor():
    query = {
        "match": {
            "status": "enable"
        }
    }
    result, items = search_index("data_drift_monitor_setting", query)
    if result is False:
        logger.warning(items)
    else:
        producer = makeproducer()
        if producer is False:
            logger.warning('kafka.errors.NoBrokersAvailable: NoBrokersAvailable')
        else:
            for item in items:
                try:
                    produceKafka(producer, item, 'drift')
                except Exception as err:
                    logger.warning(err)


def accuracy_monitor():
    query = {
        "match": {
            "status": "enable"
        }
    }
    result, items = search_index("accuracy_monitor_setting", query)
    if result is False:
        logger.warning(items)
    else:
        producer = makeproducer()
        if producer is False:
            logger.warning('kafka.errors.NoBrokersAvailable: NoBrokersAvailable')
        else:
            for item in items:
                try:
                    produceKafka(producer, item, 'accuracy')
                except Exception as err:
                    logger.warning(err)


def service_monitor():
    query = {
        "match": {
            "status": "enable"
        }
    }
    result, items = search_index("servicehealth_monitor_setting", query)
    if result is False:
        logger.warning(items)
    else:
        producer = makeproducer()
        if producer is False:
            logger.warning('kafka.errors.NoBrokersAvailable: NoBrokersAvailable')
        else:
            for item in items:
                try:
                    produceKafka(producer, item, 'service')
                except Exception as err:
                    logger.warning(err)


def search_index(index, query):
    try:
        items = ES.search(index=index, query=query, scroll='30s', size=100)
        sid = items['_scroll_id']
        fetched = items['hits']['hits']
        total = []

        for i in fetched:
            total.append(i)
        while len(fetched) > 0:
            items = ES.scroll(scroll_id=sid, scroll='30s')
            fetched = items['hits']['hits']
            for i in fetched:
                total.append(i)
            time.sleep(0.001)

        return True, total
    except exceptions.BadRequestError as err:
        return False, err
    except Exception as err:
        return False, err
