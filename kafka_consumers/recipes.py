import consumer
import parse_consumption
import producer
from time import sleep
from confluent_kafka import Consumer


if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'
    kafka_consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'auto.offset.reset': 'earliest', "group.id": "mygroup"})
    kafka_consumer.subscribe(['raw_recipes'])
    msgs = kafka_consumer.consume(1)
    # consumer.subscribe_and_consume(kafka_consumer, 'raw_recipes')
    for msg in msgs:
        html = msg.value()
        result = parse_consumption.parse_recipe(html)
        parsed_records.append(result)
    kafka_consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        kafka_producer = producer.connect()
        for rec in parsed_records:
            producer.publish(kafka_producer, parsed_topic_name, rec)

