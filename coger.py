from selenium import webdriver
from confluent_kafka import Producer
import time

# URL de Yahoo Finance para el IBEX 35
url = "https://finance.yahoo.com/quote/%5EIBEX/components?p=%5EIBEX"

# Inicializar el driver de Selenium (asegúrate de tener el WebDriver correspondiente)
driver = webdriver.Chrome(executable_path="path_to_chromedriver")

# Abrir la página web
driver.get(url)

# Esperar un momento para que se cargue la página
time.sleep(5)

# Encontrar los elementos que contienen los nombres de las empresas y sus precios
empresas = driver.find_elements_by_css_selector("td[data-test='quote-header-name']")
precios = driver.find_elements_by_css_selector("td[data-test='quote-header-prevclose']")

# Imprimir los nombres de las empresas y sus precios
conf = {'bootstrap.servers': "localhost:29092"}
producer = Producer(conf)
topic = "nuevo-dato"

for empresa, precio in zip(empresas, precios):
    print(empresa.text, precio.text)
    message = f"{empresa.text}|{precio.text}"
    producer.produce(topic, message.encode('utf-8'))

# Cerrar el driver
driver.quit()
