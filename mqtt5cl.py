from awsiot import mqtt5_client_builder
from awscrt import mqtt5
from concurrent.futures import Future
import time
import json
import os.path
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class Mqtt5Pub:
    __future_stopped = Future()
    __future_connection_success = Future()

    __TIMEOUT = 100

    def __init__(self, topic, port, endpoint, cert, pvkey, ca, clid):
        logger.info("Starting MQTT5 Publish Client")
        self.__message_topic = topic

        # Create MQTT5 client
        self.__client = mqtt5_client_builder.mtls_from_path(
            endpoint=endpoint,
            port=port,
            cert_filepath=os.path.expanduser(cert),
            pri_key_filepath=os.path.expanduser(pvkey),
            ca_filepath=os.path.expanduser(ca),
            on_lifecycle_stopped=self.__on_lifecycle_stopped,
            on_lifecycle_connection_success=self.__on_lifecycle_connection_success,
            on_lifecycle_connection_failure=self.__on_lifecycle_connection_failure,
            client_id=clid,
        )
        logger.info(f"Connecting to {endpoint} with client ID '{clid}'...")

        self.__client.start()
        lifecycle_connect_success_data = self.__future_connection_success.result(
            self.__TIMEOUT
        )
        connack_packet = lifecycle_connect_success_data.connack_packet
        logger.info(
            f"Connected to endpoint:'{endpoint}' with Client ID:'{clid}' with reason_code:{repr(connack_packet.reason_code)}"
        )

    def __del__(self):
        self.__client.stop()
        self.__future_stopped.result(self.__TIMEOUT)

    # Callback for the lifecycle event Stopped
    def __on_lifecycle_stopped(
        self, lifecycle_stopped_data: mqtt5.LifecycleStoppedData
    ):
        logger.info("Lifecycle Stopped")
        self.__future_stopped.set_result(lifecycle_stopped_data)

    # Callback for the lifecycle event Connection Success
    def __on_lifecycle_connection_success(
        self,
        lifecycle_connect_success_data: mqtt5.LifecycleConnectSuccessData,
    ):
        logger.info("Lifecycle Connection Success")
        self.__future_connection_success.set_result(lifecycle_connect_success_data)

    # Callback for the lifecycle event Connection Failure
    def __on_lifecycle_connection_failure(
        self,
        lifecycle_connection_failure: mqtt5.LifecycleConnectFailureData,
    ):
        logger.error("Lifecycle Connection Failure")
        logger.error(
            "Connection failed with exception:{}".format(
                lifecycle_connection_failure.exception
            )
        )

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5)
    )
    def publish(self, message_json):
        publish_future = self.__client.publish(
            mqtt5.PublishPacket(
                topic=self.__message_topic,
                payload=json.dumps(message_json),
                qos=mqtt5.QoS.AT_LEAST_ONCE,
            )
        )
        logger.info(f"MQTT Publishing to {self.__message_topic}: {message_json}")
        publish_completion_data = publish_future.result(self.__TIMEOUT)
        logger.info(
            f"-> PubAck received with {str(publish_completion_data.puback.reason_code)}"
        )


if __name__ == "__main__":
    load_dotenv()

    t = "test/topic"

    mqttcl = Mqtt5Pub(
        topic=t,
        port=8883,
        endpoint=os.getenv("DHT11_AWSIOT_ENDPOINT"),
        cert=os.getenv("DHT11_AWSIOT_CERT"),
        pvkey=os.getenv("DHT11_AWSIOT_PVKEY"),
        ca=os.getenv("DHT11_AWSIOT_CA"),
        clid=os.getenv("DHT11_AWSIOT_CLID"),
    )

    # Publish message
    try:
        while True:
            message_json = {"msg": "Hello!.", "value": 100}
            result = mqttcl.publish(message_json)
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Stopping Client")
        del mqttcl
        logger.info("Client Stopped!")
