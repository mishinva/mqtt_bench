import paho.mqtt.client as paho
from multiprocessing import Pool, cpu_count
import time

broker = "localhost"
port = 1883
clients_list = []
user = 'guest'
password = 'guest'
msg_count = 100000
process_count = 8


class ConnectionElement():
    def __init__(self, name):
        self.name = name
        self.client = paho.Client(name)
        self.counter = 0
        self.client.username_pw_set(user, password)
        self.client.connect(broker, port, keepalive=120)

    def publish(self):
        for i in range(0, msg_count):
            self.client.publish("test", "Data: {0}".format(i), qos=0, retain=False)
            self.client.loop(30, 100)
            print(self.name, i)

        for i in range(0, 100):
            self.client.publish("test", "Data: {0}".format(i), qos=0, retain=False)
            self.client.loop(30, 100)
            print(self.name, i)
            time.sleep(2)

def pub_test(name):
    ConnectionElement(str(name)).publish()


if __name__ == '__main__':
    p = Pool(process_count)
    p.map(pub_test, [str(i) for i in range(0, process_count)])

