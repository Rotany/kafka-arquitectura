import os
from dotenv import load_dotenv
from etoro_api import Etoro
from confluent_kafka import Consumer, KafkaError, Producer

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de las credenciales de eToro
etoro_username = os.getenv("ETORO_USERNAME")
etoro_password = os.getenv("ETORO_PASSWORD")

# Crear una instancia de la API de eToro
etoro = Etoro()

# Iniciar sesión en eToro
etoro.login(etoro_username, etoro_password)

# Función para comprar una acción en eToro
def comprar_accion(accion):
    # Intentar comprar la acción en eToro
    try:
        etoro.trade_instrument(accion, action="Buy", amount=100)  # Compra 100 unidades de la acción
        print(f"Se ha comprado la acción {accion} en eToro.")
    except Exception as e:
        print(f"No se pudo comprar la acción {accion}: {e}")

# Consumir mensajes del topic "comprar-accion" en Kafka y comprar la acción correspondiente en eToro
def consumir_y_comprar():
    # Configuración del consumidor Kafka
    conf_consumer = {
        'bootstrap.servers': 'localhost:29092',  # Cambia esto por la dirección de tu servidor Kafka
        'group.id': 'compra_group',
        'auto.offset.reset': 'earliest'
    }

    # Crea el consumidor
    consumer = Consumer(conf_consumer)

    # Suscribirse al topic "comprar-accion"
    consumer.subscribe(['comprar-accion'])

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
            accion = msg.value().decode('utf-8')  # Decodifica el mensaje
            # Compra la acción en eToro
            comprar_accion(accion)

    except KeyboardInterrupt:
        pass

    finally:
        # Cierra el consumidor
        consumer.close()

# Ejecutar la función para consumir mensajes y comprar acciones
consumir_y_comprar()
