from kafka import KafkaConsumer,KafkaProducer
from concurrent.futures import ThreadPoolExecutor
import logging as LOGGER
import requests
import time

starttime=time.time()
kafka_topic = 'loka'
kafka_retry_topic=  'loka_retry'
kafka_server = 'localhost:9092'
header_separator = '|'
consumer_group = 'csv_reader'
API_ENDPOINT = "http://127.0.0.1:8080/save_data"

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[kafka_server], api_version=(0, 10))
    except Exception as e:
         LOGGER.exception("Error connecting to kafka")
    finally:
        return _producer

def connect_kafka_consumer(topic):
    try:
        consumer = KafkaConsumer(topic,
        bootstrap_servers=[kafka_server],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=consumer_group,
        max_poll_interval_ms=500000)   
    except Exception as e:
        LOGGER.exception("Error connecting to kafka")
    finally:
        return consumer

def process_topic():
    
    consumer = connect_kafka_consumer(kafka_topic)
    producer = connect_kafka_producer()
    for msg in consumer:
        msg_received = msg.value.decode("utf-8")
        data = pre_process_message(msg_received)
        try:
            response = requests.post(url=API_ENDPOINT, json=data)
            if response.status_code == 200:
                consumer.commit()
            elif response.status_code == 500:
                producer.send(kafka_retry_topic,value=bytes(msg_received,encoding='utf-8'))
        except Exception as e:
            LOGGER.exception("Problem sending the value to the microservice, the system will try again later")
            producer.send(kafka_retry_topic,value=bytes(msg_received,encoding='utf-8'))
    consumer.close()
    producer.close()

def process_retry_topic():
    consumer = connect_kafka_consumer(kafka_retry_topic)
    while True:
        time.sleep(200.0 - ((time.time() - starttime) % 200.0))
        for msg in consumer:
            msg_received = msg.value.decode("utf-8")
            data = pre_process_message(msg_received)
            try:
                requests.post(url=API_ENDPOINT, data=data)
            except Exception as e:
                LOGGER.exception("Problem sending the value to the microservice, from the retry")
    

def pre_process_message(msg_received):
        
    msg_parts = msg_received.split(header_separator)
    header = msg_parts[0].split(";;;")
    body = msg_parts[1].split(";;;")
    data = dict(zip(header,body))
    return data

def main():
    executor = ThreadPoolExecutor(max_workers=2) 
    executor.submit(process_topic())
    executor.submit(process_retry_topic())

if __name__ == "__main__":main()