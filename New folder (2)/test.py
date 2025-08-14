import paho.mqtt.client as mqtt
import time
import warnings

# Suppress DeprecationWarning
warnings.filterwarnings("ignore", category=DeprecationWarning)

# MQTT broker and topic
BROKER = "iinms.brri.gov.bd"
PORT = 1883

# Gateway IDs
VALID_USERS = {
    "001061": "04883272217680",
    "001130": "04496B72217680",
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker.")
        for gwid in VALID_USERS:
            pub_topic = f"PIT/RF/{gwid}"
            response = "query:opens"
            client.publish(pub_topic, response)
            print(f"> Sent '{response}' to {pub_topic}")
        
        # Disconnect after short delay to ensure message is sent
        time.sleep(1)
        client.disconnect()
    else:
        print(f"Failed to connect. Return code: {rc}")

# Create client
client = mqtt.Client()
client.on_connect = on_connect

# Connect and run
try:
    client.connect(BROKER, PORT, 60)
    client.loop_start()
    time.sleep(2)  # Wait for connection and message delivery
    client.loop_stop()
except Exception as e:
    print(f"Error: {e}")
