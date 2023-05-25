import time
import pika

#conexion a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
#Declaracion de colas de mensajes
channel.queue_declare(queue="africa_queue")

#Esta funcion procesa los datos recibidos desde el servidor
def procedure(ch,method,propierties,body):
    print(f"Consumer {method.consumer_tag} recibido ->{body.decode()}")
    time.sleep(body.count(b"."))

num_consumers = 1
for i in range(num_consumers):
    channel.basic_consume(queue="africa_queue",on_message_callback=procedure,auto_ack=True)

try:
    print("Esperando mensajes")
    channel.start_consuming()
except KeyboardInterrupt:
    print("Deteniendo los consumidores")
    channel.stop_consuming()
connection.close()
