from confluent_kafka import Consumer, KafkaError, Producer

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'revisar_group',
    'auto.offset.reset': 'earliest'
}
conf_producer = {
    'bootstrap.servers': 'localhost:29092'  # Cambia esto por la dirección de tu servidor Kafka
}
# Crea el consumidor
consumer = Consumer(conf)
producer = Producer(conf_producer)

# Suscribirse al topic "nuevo-dato"
consumer.subscribe(['nuevo-dato'])

def evaluar_compra(dato):
    # Aquí debes implementar la lógica para evaluar si es una buena idea comprar
    # Puedes considerar diferentes criterios como el precio, la tendencia del mercado, etc.
    # Retorna True si es una buena idea comprar o False en caso contrario
    # Por ahora, este ejemplo simplemente retorna True
    return True

try:
    while True:
        # Espera por mensajes
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Procesa el mensaje
        dato = msg.value().decode('utf-8')  # Decodifica el mensaje
        # Aquí realizarías la lógica para evaluar si es una buena idea comprar o no
        if evaluar_compra(dato):
            producer.produce('comprar-accion', dato.encode('utf-8'))
            print("Es una buena idea comprar las acciones:", dato)
        else:
            print("No es una buena idea comprar las acciones:", dato)

except KeyboardInterrupt:
    pass

finally:
    # Cierra el consumidor
    consumer.close()


