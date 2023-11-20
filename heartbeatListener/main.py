from flask import Flask, request, jsonify
from kafka import KafkaProducer
from flask_cors import CORS
from dotenv import dotenv_values
import uuid
from threading import Event
import signal
import json


from flask_kafka import FlaskKafka
app = Flask(__name__)
CORS(app)

INTERRUPT_EVENT = Event()
config = dotenv_values("../.env")
config['topics'] = json.loads(config['topics'])

bus = FlaskKafka(INTERRUPT_EVENT,
                bootstrap_servers=config['bootstrap_server'],
                group_id="bd_project",
                value_deserializer = lambda v: json.loads(v.decode('utf-8'))
                )
producer = KafkaProducer(bootstrap_servers=config['bootstrap_server'],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))

@bus.handle(config['topics']['test_config'])
def test_config_handler(msg):
    print("consumed {} from test-topic".format(msg))

def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)

if __name__ == '__main__':

    bus.run()
    listen_kill_server()
    port = config['orchestrator_port']
    app.run(debug=True, threaded=True, port=port)