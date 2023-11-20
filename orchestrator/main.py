from flask import Flask, jsonify
from kafka import KafkaProducer
from flask_kafka import FlaskKafka
from flask_cors import CORS
from dotenv import dotenv_values
from threading import Event, Lock
from colorama import Fore, Style
from datetime import datetime
from flask_socketio import SocketIO, emit
from time import sleep
from numpy import mean, median
from threading import Thread

import subprocess
import os
import bisect
import signal
import json
import uuid



app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*", engineio_logger=True)

INTERRUPT_EVENT = Event()
config = dotenv_values("../.env")
config['topics'] = json.loads(config['topics'])
config['default_driver_count'] = int(config['default_driver_count'])

bus = FlaskKafka(INTERRUPT_EVENT,
                bootstrap_servers=config['bootstrap_server'],
                group_id="bd_project_orchestrator",
                value_deserializer = lambda v: json.loads(v.decode('utf-8')),
                # auto_offset_reset='earliest'
                )
producer = KafkaProducer(bootstrap_servers=config['bootstrap_server'],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))

current_test_id = None
metricsStore = [{}, Lock()]

@socketio.on('connect')
def on_connect():
    emit("data", "Sucess", broadcast=True)

@socketio.on('stream_data')
def on_stream():
    # if current_test_id is not None:
        data = {
            'reports': metricsStore[0][current_test_id]['reports'], 
            "metrics": calculateMertics(metricsStore[0][current_test_id]['time_list']),
            # "time_taken": calculateTimeTaken(metricsStore[0][current_test_id]['timestamps'])
        }
        emit("stream_data", "Sucess", broadcast=True)
        sleep(60)

def calculateMertics(time_list):
    return {
        "mean": mean(time_list),
        "median": median(time_list),
        "min": min(time_list),
        "max": max(time_list),
    }


# @socketio.on('data')
# def on_data():
#     emit("data", metricsStore[0][])
#     sleep(60)

@app.route("/metrics/<string:test_id>", methods=['POST'])
def metrics(test_id):
    print(test_id)
    metricsStore[1].acquire()
    print(metricsStore)
    if test_id in metricsStore[0]:
        context = {
            'reports': metricsStore[0][test_id]['reports'], 
            "metrics": calculateMertics(metricsStore[0][test_id]['time_list']),
            # "time_taken": calculateTimeTaken(metricsStore[0][current_test_id]['timestamps'])
        }
        metricsStore[1].release()
        return jsonify(context) 
    else:
        metricsStore[1].release()
        return jsonify({'metrics': ''})

@app.route("/avalance/", methods=['POST'])
@app.route("/avalance/<int:count>", methods=['POST'])
def avalanche(count = config['default_driver_count']):
    context = {
        "test_type": "AVALANCE",
        "test_id": str(uuid.uuid4()),
        "test_message_delay": 0,
        "message_count_per_driver": count
    }
    producer.send(config['topics']['test_config'], context)
    producer.flush()
    return jsonify(context) 

@app.route("/tsunami/", methods=['POST'])
@app.route("/tsunami/<float:delay>", methods=['POST'])
@app.route("/tsunami/<int:count>/<float:delay>", methods=['POST'])
@app.route("/tsunami/<int:count>/<int:delay>", methods=['POST'])
def tsunami(count=config['default_driver_count'], delay=0):
    delay = float(delay)
    context = {
        "test_type": "TSUNAMI",
        "test_id": str(uuid.uuid4()),
        "test_message_delay": delay,
        "message_count_per_driver": count
    }
    producer.send(config['topics']['test_config'], context)
    producer.flush()
    return jsonify(context) 

@app.route("/metricsStore", methods= ['POST'])
def metricsStoreVal():
    print(metricsStore[0])
@app.route("/trigger/", methods=['POST'])
@app.route("/trigger/<string:test_id>", methods=['POST'])
def trigger(test_id):
    if test_id:
        context = {
            "test_id": test_id,
            "trigger": "YES"
        }
        producer.send(config['topics']['trigger'], context)
        producer.flush()
        current_test_id = test_id
        return jsonify(context) 
    else:
        return "No test_id", 400

registeredNodes = [{}, Lock()]

@bus.handle(config['topics']['register'])
def register_handler(msg):
    data = msg.value
    registeredNodes[1].acquire() 
    registeredNodes[0][data["node_id"]] = {
        "node_id": data['node_id'],
        "node_IP": data["node_IP"],
        "alive": True,
        "last_online": datetime.now()
    }
    registeredNodes[1].release() 
    print(f"""
          {Fore.RED}[{config['topics']['register']}]: {Style.RESET_ALL} Registered {data['node_id']} with IP {data['node_IP']}
          """)

@bus.handle(config['topics']['heartbeat'])
def heartbeat_handler(msg):
    data = msg.value
    registeredNodes[1].acquire() 
    if data['node_id'] in registeredNodes[0]:
        registeredNodes[0][data["node_id"]].update({
            "alive": True,
            "last_online": datetime.now()
        })
    
    registeredNodes[1].release() 
    print(f"""
          {Fore.RED}[{config['topics']['heartbeat']}]: {Style.RESET_ALL} Heartbeat Recieved {data['node_id']}
          """)


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
    print(metricsStore)
    print(f"""
        {Fore.RED}[{config['topics']['metrics']}]: {Style.RESET_ALL} Metrics Recieved\n
        \t{json.dumps(metricsStore[0][data['test_id']]['time_list'], indent=2)}
    """)

def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)

def aliveChecker():
    while True:
        current_time = datetime.now()
        timeout = 2 * 60
        registeredNodes[1].acquire()
        for node_id in registeredNodes[0]:
            if registeredNodes[0][node_id]['alive']:
                last_online = registeredNodes[0][node_id]['last_online']
                time_diff = (current_time - last_online).total_seconds()
                if time_diff > timeout:
                    registeredNodes[0][node_id].update({
                        "alive": False,
                    })
                    print(f"""
                        {Fore.RED}[Heartbeat Checker]: Havent Recieved heartbeat from {node_id} with ip {registeredNodes[0][node_id]['node_IP']}
                    """)
        registeredNodes[1].release()
        sleep(2 * 60)

def start_independent_process():
    # Replace 'your_script.py' with the script or command you want to run
    process = subprocess.Popen(['python', 'your_script.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
    return process

def stop_process(process):
    try:
        # Send SIGTERM signal to the process group
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        process.communicate()  # Wait for the process to finish
    except ProcessLookupError:
        # The process may have already terminated
        pass


if __name__ == '__main__':
    bus.run()
    listen_kill_server()
    port = config['orchestrator_port']
    thread = Thread(target=aliveChecker)
    try:
        thread.start()
        socketio.run(app, debug=True,  host='0.0.0.0', port=port)
    except:
        thread.join()