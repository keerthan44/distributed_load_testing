from flask import Flask, jsonify
from kafka import KafkaProducer
from flask_kafka import FlaskKafka
from flask_cors import CORS
from dotenv import dotenv_values
from threading import Event, Lock, Thread, activeCount
from colorama import Fore, Style
from datetime import datetime
from numpy import mean, median
from time import sleep
from variableListener import VariableListener

import bisect
import requests
import signal
import json
import uuid
import socket

def get_lan_ip_address():
    try:
        # Create a socket to get the local machine's hostname
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip_address = s.getsockname()[0]
        s.close()

        return local_ip_address
    except Exception as e:
        print(f"An error occurred: {e}")
        return "127.0.0.1"

node_ip = get_lan_ip_address()
node_id = str(uuid.uuid4())

app = Flask(__name__)
CORS(app)

INTERRUPT_EVENT = Event()
config = dotenv_values("../.env")
config['topics'] = json.loads(config['topics'])
config['default_driver_count'] = int(config['default_driver_count'])

bus = FlaskKafka(INTERRUPT_EVENT,
                bootstrap_servers=config['bootstrap_server'],
                group_id=f"bd_project_driver_{uuid.uuid4()}",
                value_deserializer = lambda v: json.loads(v.decode('utf-8')),
                # auto_offset_reset='earliest'
                )
producer = KafkaProducer(bootstrap_servers=config['bootstrap_server'],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))

tests = [{}, Lock()]

@bus.handle(config['topics']['test_config'])
def testConfig_handler(msg):
    data = msg.value
    tests[1].acquire()
    test_id = data['test_id']
    tests[0][test_id] = data
    tests[1].release()
    print(f"""
          {Fore.RED}[{config['topics']['test_config']}]: {Style.RESET_ALL} {test_id} test recieved.\n\t\t\t  {json.dumps(tests[0][test_id])}
          """)

metricsStore = [{}, Lock()]

@bus.handle(config['topics']['metrics'])
def metrics_handler(msg):
    data = msg.value
    # registeredNodes[1].acquire()
    # if data['node_id'] not in registeredNodes[0]:
    #     registeredNodes[1].release()
    #     return 
    # registeredNodes[1].release()

    reportData = {
        'report_id': data['report_id'],
        'time': data['time']
    }
    metricsStore[1].acquire() 
    if data['test_id'] in metricsStore[0]:
        exisitng_metrics = metricsStore[0][data['test_id']]
        exisitng_metrics['reports'].append(reportData),
        bisect.insort(exisitng_metrics['time_list'], data['time']),
        exisitng_metrics['timestamps'].append(datetime.now())
    else:
        metricsStore[0][data['test_id']] = {
            "reports": [reportData],
            "time_list": [data['time']],
            "timestamps": [datetime.now()]
        }
    metricsStore[1].release() 
    print(f"""
        {Fore.RED}[{config['topics']['metrics']}]: {Style.RESET_ALL} Metrics Recieved\n
        \t{json.dumps(metricsStore[0][data['test_id']]['time_list'], indent=2)}
    """)

@bus.handle(config['topics']['trigger'])
def trigger_handler(msg):
    data = msg.value
    tests[1].acquire()
    try:
        testData = tests[0][data['test_id']]
    except:
        tests[1].release()
        return
    tests[1].release()
    if testData:
        if testData['test_type'] == 'TSUNAMI':
            print("TSUNAMI")
            TSUNAMI(testData)
        else:
            print("AVALANCE")
            AVALANCE(testData)
        print(f"""
            {Fore.RED}[{config['topics']['trigger']}]: {Style.RESET_ALL} Trigger for {data['test_id']} recieved
            """)

def TSUNAMI(testData):
    def run():
        start_time = datetime.now()
        try:
            requests.get(config['target_server'] + "/metrics")
        except:
            return

        end_time = datetime.now()

        # Calculate the runtime in milliseconds

        runtime_ms = (end_time - start_time).total_seconds() * 1000
        producer = KafkaProducer(bootstrap_servers=config['bootstrap_server'],
                                    value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        context = {
            'report_id': "report_" + str(uuid.uuid4()),
            'time': runtime_ms,
            'test_id': testData['test_id'],
        }
        
        producer.send(config['topics']['metrics'], context)
        producer.flush()
        return 0

    status = False
    count = 0
    while count < 3:
        response = requests.get("http://127.0.0.1:8080/ping")
        if response.status_code == 200:
            status = True
            break
        count += 1
    if status:
        threadList = [Thread(target=run)]
        for i in range(testData['message_count_per_driver']):
            threadList[i].start()
            sleep(testData['test_message_delay'] / 1000)
            threadList.append(Thread(target=run))

def AVALANCE(testData):
    def run():
        start_time = datetime.now()
        try:
            requests.get(config['target_server'] + "/metrics")
        except:
            return

        end_time = datetime.now()

        # Calculate the runtime in milliseconds

        runtime_ms = (end_time - start_time).total_seconds() * 1000
        producer = KafkaProducer(bootstrap_servers=config['bootstrap_server'],
                                    value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        context = {
            'report_id': "report_" + str(uuid.uuid4()),
            'time': runtime_ms,
            'test_id': testData['test_id'],
        }
        
        producer.send(config['topics']['metrics'], context)
        producer.flush()
        return 0

    status = False
    count = 0
    while count < 3:
        response = requests.get("http://127.0.0.1:8080/ping")
        if response.status_code == 200:
            status = True
            break
        count += 1
    if status:
        def done():
            print("Done")
        threadList = [Thread(target=run)]
        for i in range(testData['message_count_per_driver']):
            threadList[i].start()
            threadList.append(Thread(target=run))
        threadCount = VariableListener(activeCount, done)



        print(f"""
            {Fore.RED}[AVALANCE]: {Style.RESET_ALL} Target Server Up 
            """)
        
    else: 
        print(f"""
            {Fore.RED}[AVALANCE]: {Style.RESET_ALL} Target Server Down 
            """)
            

def register(port):
    data = {
        'node_id': node_id,
        'node_IP': node_ip + ":" + str(port),
        'timestamp': str(datetime.now())
    }
    producer.send(config['topics']['register'], data)
    producer.flush()
    print(f"""
        {Fore.RED}[Registered]: {Style.RESET_ALL} Registered 
        """)

def calculateMetrics(time_list):
    context = {
        "mean": mean(time_list),
        "median": median(time_list),
        "maximum": max(time_list),
        "minimum": min(time_list),
    }

# thread_event = Event()

# def startBackgroundTask():
#     def backgroundTask():
#         while thread_event.is_set():
            
#             sleep(100)
#     try:
#         thread_event.set()
        
#         thread = Thread(target=backgroundTask)
#         thread.start()

#         return "Background task started!"
#     except Exception as error:
#         return str(error)

def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)

def get_available_port(port):
    # Check if the specified port is already in use
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                break
            except OSError as e:
                port += 1
    return port

port = get_available_port(int(config['driver_port']))
with app.app_context():
    register(port)

def triggerHeartbeat():
    while True:
        context = {
            'node_id': node_id,
        }
        producer.send(config['topics']['heartbeat'], context)
        sleep(10)
    

if __name__ == '__main__':
    bus.run()
    listen_kill_server()
    thread = Thread(target=triggerHeartbeat)
    try:
        thread.start()
        app.run(debug=True, use_reloader=False, threaded=True, host='0.0.0.0', port=port)
    except:
        thread.join()
        