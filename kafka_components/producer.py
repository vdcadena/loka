from kafka import KafkaProducer
import csv
import traceback
import os.path

my_path = os.path.abspath(os.path.dirname(__file__))
csv_file = os.path.join(my_path, "../files/sample_us.tsv")
kafka_topic = 'loka'
kafka_server = 'localhost:9092'
header_separator = "|"


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[kafka_server], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def stream_csv():
    producer = connect_kafka_producer()
    with open(csv_file) as csvfile:
        readCSV = csv.reader(csvfile, delimiter='\t')
        header = next(readCSV,None)
        header_str = ";;;".join(header)
        for row in readCSV:
            try:  
                row_str = ";;;".join(row)     
                row_ = header_str + header_separator + row_str
                value_to_send = bytes(row_, encoding='utf-8')      
                producer.send(kafka_topic,value=value_to_send)
            except:
                traceback.print_exc()


def main():
    stream_csv()


if __name__ == "__main__":main()