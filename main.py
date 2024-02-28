from confluent_kafka import Producer
import datetime

def delivery_report(err, msg):
    if err is not None:
        print(f'Error: Mensaje no entregado: {err}')
    else:
        print(f'Mensaje entregado: {msg.value().decode("utf-8")}')

def produce_message(producer, topic, message):
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.poll(0)

def main():
    conf = {'bootstrap.servers': "localhost:29092"}
    topic = 'hello_kafka'
    producer = Producer(conf)

    while True:
        name = input("Ingresa tu nombre: ")
        if name == "e":
            producer.flush()
            break

        now = datetime.datetime.now()
        message = f"{name} conectado en {now}"
        produce_message(producer, topic, message)

if __name__ == "__main__":
    main()
