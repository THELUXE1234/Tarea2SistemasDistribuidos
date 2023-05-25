import json
import time
import random
from threading import Thread
from kafka import KafkaProducer

# Configuración del productor de Kafka
bootstrap_servers = 'localhost:9092'  # Dirección del servidor Kafka
topic = ['asia_topic','africa_topic','europe_topic','oceania_topic','america_topic'] # Nombre del tema al que se enviarán los mensajes


# Clase IoTDevicetopic
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
        producer1 = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        producer2 = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        producer3 = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        producer4 = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        producer5 = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        while self.is_running:
            timestamp = self.generate_timestamp()
            value1 = self.generate_asia()
            value2 = self.generate_europa()
            value3 = self.generate_america()
            value4 = self.generate_africa()
            value5 = self.generate_oceania()
            data1 = {
                'Timestamp': timestamp,
                'Values': value1
            }
            data2 = {
                'Timestamp': timestamp,
                'Values': value2
            }
            data3 = {
                'Timestamp': timestamp,
                'Values': value3
            }
            data4 = {
                'Timestamp': timestamp,
                'Values': value4
            }
            data5 = {
                'Timestamp': timestamp,
                'Values': value5
            }
            json_data1 = json.dumps(data1)
            json_data2 = json.dumps(data2)
            json_data3 = json.dumps(data3)
            json_data4 = json.dumps(data4)
            json_data5 = json.dumps(data5)
            print(f"Device {self.device_id} - {json_data1}") 
            print(f"Device {self.device_id} - {json_data2}") 
            print(f"Device {self.device_id} - {json_data3}") 
            print(f"Device {self.device_id} - {json_data4}") 
            print(f"Device {self.device_id} - {json_data5}") 
            start_time = time.time()
            producer1.send(topic[0], value=data1)
            producer2.send(topic[2], value=data2)
            producer3.send(topic[4], value=data3)
            producer4.send(topic[1], value=data4)
            producer5.send(topic[3],value=data5)
            end_time = time.time()
            duracion = end_time-start_time
            print(f"TIME RESPONDE: {duracion}")
            time.sleep(self.delta_t)
        producer1.close()
        producer2.close()
        producer3.close()
        producer4.close()
        producer5.close()

    def stop(self):
        self.is_running = False


# Número de dispositivos IoT que deseas simular
num_devices = 5

# Parámetros para cada dispositivo
delta_t = 0.2 # Intervalo de tiempo entre envío de información (en segundos)
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