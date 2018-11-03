from confluent_kafka import Consumer, KafkaError
import sys


def connect():
    try:
        c = Consumer({'bootstrap.servers': 'localhost:9092', 'auto.offset.reset': 'earliest', "group.id": "mygroup"})
    except Exception as ex:
        print('exception while connecting kafka')
        raise ex
    return c


def subscribe_and_consume(consumer: Consumer, topic: str) -> None:
    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)
        # msg.offset()

    # Subscribe to topics
    consumer.subscribe([topic], on_assign=print_assignment)


    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Received message: {}'.format(msg.offset()))
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
