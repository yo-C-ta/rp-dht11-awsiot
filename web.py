import RPi.GPIO as GPIO
import time
import os
import logging
from bottle import route, run
import threading
import datetime
from mqtt5cl import Mqtt5Pub
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# DHT11 connect to BCM_GPIO14
DHTPIN = 14
MAX_UNCHANGE_COUNT = 100

STATE_INIT_PULL_DOWN = 1
STATE_INIT_PULL_UP = 2
STATE_DATA_FIRST_PULL_DOWN = 3
STATE_DATA_PULL_UP = 4
STATE_DATA_PULL_DOWN = 5


def read_dht11_dat():
    GPIO.setup(DHTPIN, GPIO.OUT)
    GPIO.output(DHTPIN, GPIO.HIGH)
    time.sleep(0.05)
    GPIO.output(DHTPIN, GPIO.LOW)
    time.sleep(0.02)
    GPIO.setup(DHTPIN, GPIO.IN, GPIO.PUD_UP)

    unchanged_count = 0
    last = -1
    data = []
    while True:
        current = GPIO.input(DHTPIN)
        data.append(current)
        if last != current:
            unchanged_count = 0
            last = current
        else:
            unchanged_count += 1
            if unchanged_count > MAX_UNCHANGE_COUNT:
                break

    state = STATE_INIT_PULL_DOWN

    lengths = []
    current_length = 0

    for current in data:
        current_length += 1

        if state == STATE_INIT_PULL_DOWN:
            if current == GPIO.LOW:
                state = STATE_INIT_PULL_UP
            else:
                continue
        if state == STATE_INIT_PULL_UP:
            if current == GPIO.HIGH:
                state = STATE_DATA_FIRST_PULL_DOWN
            else:
                continue
        if state == STATE_DATA_FIRST_PULL_DOWN:
            if current == GPIO.LOW:
                state = STATE_DATA_PULL_UP
            else:
                continue
        if state == STATE_DATA_PULL_UP:
            if current == GPIO.HIGH:
                current_length = 0
                state = STATE_DATA_PULL_DOWN
            else:
                continue
        if state == STATE_DATA_PULL_DOWN:
            if current == GPIO.LOW:
                lengths.append(current_length)
                state = STATE_DATA_PULL_UP
            else:
                continue
    if len(lengths) != 40:
        logger.debug("Data not good, skip")
        return False

    shortest_pull_up = min(lengths)
    longest_pull_up = max(lengths)
    halfway = (longest_pull_up + shortest_pull_up) / 2
    bits = []
    the_bytes = []
    byte = 0

    for length in lengths:
        bit = 0
        if length > halfway:
            bit = 1
        bits.append(bit)
    logger.debug(f"bits: {bits}, length: {len(bits)}")
    for i in range(0, len(bits)):
        byte <<= 1
        if bits[i]:
            byte |= 1
        else:
            byte |= 0
        if (i + 1) % 8 == 0:
            the_bytes.append(byte)
            byte = 0
    logger.debug(the_bytes)
    checksum = (the_bytes[0] + the_bytes[1] + the_bytes[2] + the_bytes[3]) & 0xFF
    if the_bytes[4] != checksum:
        logger.debug("Data not good, skip")
        return False

    return the_bytes[0], the_bytes[2]


temp = 0
humi = 0
disc = 0
now = datetime.datetime.now()


@route("/")
def root():
    return f"""<!doctype html>
            <html lang="ja">
            <head>
            <link rel="icon" href="data:,">
            <meta charset="UTF-8">
            <title>Temp&Humi</title>
            </head>
            <body>
            <p>気温 {temp} ℃</p>
            <p>湿度 {humi} % </p>
            <p>不快指数 {disc} (快適範囲 70-80）</p>
            <p>{now.isoformat()}</p>
            </body>
            </html>"""


def server():
    run(host="0.0.0.0", port=8080)


def main(mqtt5cl):
    logger.info("Start DHT11 Temperature program")

    server_thread = threading.Thread(target=server, daemon=True)
    server_thread.start()

    while True:
        result = read_dht11_dat()
        if result:
            global humi, temp, disc, now
            now = datetime.datetime.now()
            humi, temp = result
            disc = round(0.81 * temp + 0.01 * humi * (0.99 * temp - 14.3) + 46.3)
            mqtt5cl.publish(
                {
                    "DEVICE": f"{os.uname().nodename},{os.uname().sysname},{os.uname().release},{os.uname().machine}",
                    "TEMP": temp,
                    "HUMI": humi,
                    "DISC": disc,
                    "TIME": now.isoformat(),
                }
            )

            time.sleep(60)
        else:
            time.sleep(5)


if __name__ == "__main__":
    GPIO.setmode(GPIO.BCM)
    load_dotenv()

    mqtt5cl = Mqtt5Pub(
        topic=os.getenv("DHT11_AWSIOT_TOPIC"),
        port=8883,
        endpoint=os.getenv("DHT11_AWSIOT_ENDPOINT"),
        cert=os.getenv("DHT11_AWSIOT_CERT"),
        pvkey=os.getenv("DHT11_AWSIOT_PVKEY"),
        ca=os.getenv("DHT11_AWSIOT_CA"),
        clid=os.getenv("DHT11_AWSIOT_CLID"),
    )

    try:
        main(mqtt5cl)
    except KeyboardInterrupt:
        GPIO.cleanup()
        del mqtt5cl
