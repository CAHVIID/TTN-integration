############################################################################
#
# MQTT to Azure Event hub bridge
#
# 10-04-2020 1.0 Initial version    Christian Anker Hviid cah@byg.dtu.dk
# Inspired by ATEA script by Bjarke Bolding Rasmussen BBRA@atea.dk
#
# Integration bridge from The Things Network via MQTT to Azure Event hub 
#
# Help sources
# http://www.steves-internet-guide.com/into-mqtt-python-client/
# https://www.eclipse.org/paho/clients/python/docs/
#
############################################################################

scriptversion = 1.0

############ includes
import schedule
import time
from datetime import datetime
#import ssl
import json
import paho.mqtt.client as mqtt
from azure.eventhub import EventHubClient, Sender, EventData

############ print

def logPrint(log):
    print(str(datetime.utcnow()) + " UTC: " + str(log))

############ numeric validation function
    
def is_numeric(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return True
    
############ MQTT on_connect event handler
# The callback for when a PUBLISH message is received from the server

def on_message(client, userdata, message):
    try:
        #print("Message received " ,str(message.payload.decode("utf-8")))
        #print("Message topic=",message.topic)
        deviceType, devices, deviceName, messageType = message.topic.split("/") #dtu_b127/devices/elsys-ers-co2-X/up
        payloadDecoded = json.loads(message.payload.decode('utf-8'))

        #Time format from TTN: '2020-04-13T07:24:08.42315423Z'
        #rpartiton looks for "." from right side, puts the elements into tuple. [0] returns first tuple element, i.e. everything to the left of the "."
        new_time = payloadDecoded['metadata']['time'].rpartition(".")[0]
        ts = round(datetime.timestamp(datetime.strptime(new_time,"%Y-%m-%dT%H:%M:%S")))
        
        # Check to see if data fields are included in message
        value = payloadDecoded['metadata']['latitude']
        if is_numeric(value):
            latitude = value
        else:
            latitude = None

        value = payloadDecoded['metadata']['longitude']
        if is_numeric(value):
            longitude = value
        else:
            longitude = None

        payload = []
        payload = {
            'deviceid': payloadDecoded['hardware_serial'],
            'devicename': payloadDecoded['dev_id'],
            'devicetype': 'elsys-ers',
            'attributes': {
                'location': payloadDecoded['app_id'],
                'latitude': latitude,
                'longitude': longitude
                },
            'telemetry': []
        }
            
        tele = {'ts': ts,
                'values': {}
                } 
        
        # Check if telemetry values are indeed numeric
        measure = payloadDecoded['payload_fields']
        for key in measure.keys():
            if is_numeric(measure[key]):
                tele['values'][key] = measure[key]
            else:
                tele['values'][key] = None

        payload['telemetry'].append(tele)

        payload['telemetry'][0]['values']['data_rate'] = str(payloadDecoded['metadata']['data_rate'])
        payload['telemetry'][0]['values']['rssi'] = int(payloadDecoded['metadata']['gateways'][0]['rssi'])
        
        data.append(payload)
    
    except Exception as e:
        print(e)
        pass

############ MQTT on_connect event handler
# The callback for when the client receives a CONNACK response from the server

def on_connect(client, userdata, flags, rc):
    returnCodes = {
        0: "Connection successful",
        1: "Connection refused – incorrect protocol version",
        2: "Connection refused – invalid client identifier",
        3: "Connection refused – server unavailable",
        4: "Connection refused – bad username or password",
        5: "Connection refused – not authorised"
    }
    logPrint(returnCodes.get(rc, "Error - Invalid return code"))
    if rc==0:
        client.connected_flag=True
        client.subscribe(broker_subscription, qos=1)
    else:
        client.bad_connection_flag=True
    
############ MQTT on_disconnect event handler 

def on_disconnect(client, userdata, rc):
    logPrint("Disconnecting reason  " + str(rc))
    client.connected_flag=False
    client.disconnect_flag=True
    
############ Send payload to Azure Event Hub function

def sendToEventHub(data):
    try:
        client = EventHubClient(
            cfg['EventHubURL'], debug=False, username=cfg['EventHubPolicyName'], password=cfg['EventHubPrimaryKey'])
        sender = client.add_sender(partition="0")
        client.run()
        try:
            count = 0
            for payload in data:
                sender.send(EventData(json.dumps(payload)))
                #logPrint("Payload sent: " + json.dumps(payload))
                count += 1
        except:
            logPrint("Send to Eventhub Failed")
            raise
        finally:
            logPrint(str(count) + " payloads sent")
            data.clear()
            client.stop()
    except KeyboardInterrupt:
        pass

############ Send batch to Event Hub function
# Notice that the interval should be longer than the TTN devices uplink interval, or else a lot of payloads will just be empty
def timedDataTransfer():
    sendToEventHub(data)

########################################

# init
mqtt.Client.connected_flag=False
mqtt.Client.bad_connection_flag=False
data = []

# load configuration
cfg = json.load(open('config.json'))
client_id = cfg['MQTTClientID']
broker_address = cfg['MQTTBrokerHost']
broker_port = cfg['MQTTBrokerPort']
broker_username = cfg['MQTTClientUsername']
broker_password = cfg['MQTTClientPassword']
#broker_certificate = cfg['MQTTCertificate']
broker_subscription = cfg['MQTTSubscription']
#broker_subscription = "#" # subscribe to everything, for debugging purpose

schedule.every(int(cfg['EventHubBatchTimer'])).seconds.do(timedDataTransfer)

# header
logPrint("MQTT to Azure Event hub bridge "+ str(scriptversion))
logPrint("MQTT broker: " + broker_address + ", port " + str(broker_port))
logPrint("Azure EventHub: " + cfg['EventHubURL'])
logPrint("Configured to send payloads every " + cfg['EventHubBatchTimer'] + " seconds")

# execute
client = mqtt.Client(client_id, clean_session=True)
#client.tls_set(broker_certificate, tls_version=ssl.PROTOCOL_TLSv1_2)
#client.tls_insecure_set(True) #True means no encryption, set to False in production environment
#client.tls_set()
client.username_pw_set(username=broker_username,password=broker_password)
client.on_connect=on_connect
client.on_message=on_message

client.loop_start()

try:
    client.connect(broker_address, broker_port, 60)
except Exception as e:
    logPrint("Connection failed: " + str(e))
    exit(1)

while not client.connected_flag and not client.bad_connection_flag:
    time.sleep(1)
if client.bad_connection_flag:
    exit(1)

client.subscribe(broker_subscription, qos=1)

while True:
    schedule.run_pending()
    time.sleep(int(cfg['EventHubBatchTimer']))