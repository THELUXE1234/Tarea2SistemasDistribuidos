import pika
import json
import time
import random
from threading import Thread

# Clase IoTDevice
class IoTDevice(Thread):
    def __init__(self, device_id, delta_t, max_value_size):
        super().__init__()
        self.device_id = device_id
        self.delta_t = delta_t
        self.max_value_size = max_value_size
        self.is_running = True

    def generate_timestamp(self):
        return int(time.time())

    def generate_asia(self):
        value = random.randint(120000, 150000)
        return value
    def generate_europa(self):
        value = random.randint(10000, 20000)
        return value
    def generate_america(self):
        value = random.randint(50000, 70000)
        return value
    def generate_africa(self):
        value = random.randint(250000, 300000)
        return value
    def generate_oceania(self):
        value = random.randint(3000, 5000)
        return value

    def run(self):
        while self.is_running:
            timestamp1 = self.generate_timestamp()
            timestamp2= self.generate_timestamp()
            timestamp3 = self.generate_timestamp()
            timestamp4 = self.generate_timestamp()
            timestamp5 = self.generate_timestamp()
            
            value1 = self.generate_asia()
            value2 = self.generate_europa()
            value3 = self.generate_america()
            value4 = self.generate_africa()
            value5 = self.generate_oceania()
            data1 = {
                'Timestamp': timestamp1,
                'Values': value1
            }
            data2 = {
                'Timestamp': timestamp2,
                'Values': value2
            }
            data3 = {
                'Timestamp': timestamp3,
                'Values': value3
            }
            data4 = {
                'Timestamp': timestamp4,
                'Values': value4
            }
            data5 = {
                'Timestamp': timestamp5,
                'Values': value5
            }
            
            json_data1 = json.dumps(data1)
            json_data2 = json.dumps(data2)
            json_data3 = json.dumps(data3)
            json_data4 = json.dumps(data4)
            json_data5 = json.dumps(data5)

            print(f"Device ASIA {self.device_id} - {json_data1}")
            print(f"Device EUROPE {self.device_id} - {json_data2}")
            print(f"Device AMERICA {self.device_id} - {json_data3}")
            print(f"Device AFRICA {self.device_id} - {json_data4}")
            print(f"Device OCEANIA {self.device_id} - {json_data5}")
            #Configuracion conexion a rabbit
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel1 = connection.channel()
            channel2 = connection.channel()
            channel3 = connection.channel()
            channel4 = connection.channel()
            channel5 = connection.channel()
            
            #Declaracion de colas de mensajes
            channel1.queue_declare(queue="asia_queue")
            channel2.queue_declare(queue="europe_queue")
            channel3.queue_declare(queue="america_queue")
            channel4.queue_declare(queue="africa_queue")
            channel5.queue_declare(queue="oceania_queue")
            start_time = time.time()
            #Publicacion de mensaje a la cola
            channel1.basic_publish(exchange="",
                                routing_key="asia_queue",
                                body=json_data1)

            channel2.basic_publish(exchange="",
                                routing_key="europe_queue",
                                body=json_data1)
            channel3.basic_publish(exchange="",
                                routing_key="america_queue",
                                body=json_data2)
            channel4.basic_publish(exchange="",
                                routing_key="africa_queue",
                                body=json_data3)
            channel5.basic_publish(exchange="",
                                routing_key="oceania_queue",
                                body=json_data4)
            end_time = time.time()
            duracion = end_time-start_time
            print(f"TIME RESPONSE:{duracion}")
            time.sleep(self.delta_t)
        #CERRAR CONECCION DE RABBIT
        connection.close()
    def stop(self):
        self.is_running = False


# Número de dispositivos IoT que deseas simular
num_devices = 1000

# Parámetros para cada dispositivo
delta_t = 0.5 # Intervalo de tiempo entre envío de información (en segundos)
max_value_size = 10  # Tamaño máximo del valor de información

devices = []
for i in range(num_devices):
    device = IoTDevice(device_id=i + 1, delta_t=delta_t, max_value_size=max_value_size)
    devices.append(device)
    device.start()
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Deteniendo la simulación de los dispositivos IoT...")
    for device in devices:
        device.stop()
        device.join()