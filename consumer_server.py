from confluent_kafka import Consumer

def run_consumer():
    try:
        c = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'dep.police.service.0',
        })

        c.subscribe(['police.service.calls.v2'])

        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt as e:
        c.close()
        print("Shutting down...")

if __name__ == '__main__':
    run_consumer()