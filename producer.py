from confluent_kafka import Producer


def connect():
    try:
        p = Producer({'bootstrap.servers': 'localhost:9092'})
    except Exception as ex:
        print('exception while connecting kafka')
        raise ex
    return p


def publish(producer: Producer, topic: str, value: str) -> None:
    # Trigger any available delivery report callbacks from previous produce() calls
    producer.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    producer.produce(topic, value.encode('utf-8'), callback=ack_produce)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


def ack_produce(err, msg):
    """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
