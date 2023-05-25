from kafka import KafkaConsumer
import json
import threading
bootstrap_servers = 'localhost:9092'  # Dirección del servidor Kafka
topic = 'oceania_topic'  # Nombre del tema al que se enviarán los mensajes

num_consumers = 5
consumers = []
def messages(consumer):
    try:
        for message in consumer:
            print(message.value)
    except KeyboardInterrupt:
        consumer.close()

try:
    for i in range(num_consumers):
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        consumers.append(consumer)
        consumer.subscribe([topic])
        thread = threading.Thread(target=messages, args=(consumer,))
        thread.start()

    # Esperar a que todos los hilos terminen
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            thread.join()
except KeyboardInterrupt:
    for consumer in consumers:
        consumer.close()







