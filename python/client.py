import os
import time
import json
import random
import zmq
import msgpack
from datetime import datetime

CLIENT_NAME = os.getenv("CLIENT_NAME", "bot_py_1")
BROKER_URL = os.getenv("BROKER_URL", "tcp://broker:5555")
CHANNELS_POOL = ["geral", "jogos", "avisos", "tecnologia", "musica"]


def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def log_message(direction, msg_dict):
    ts = msg_dict.get("data", {}).get("timestamp", time.time())
    ts_str = format_time(ts)
    print(f"[{CLIENT_NAME}] [{ts_str}] {direction}: {json.dumps(msg_dict, ensure_ascii=False)}", flush=True)


def send_and_receive(socket, payload):
    log_message("ENVIANDO", payload)
    packed = msgpack.packb(payload, use_bin_type=True)
    socket.send(packed)

    raw_reply = socket.recv()
    reply = msgpack.unpackb(raw_reply, raw=False)
    log_message("RECEBIDO", reply)
    return reply


def do_login(socket):
    while True:
        req = {
            "service": "login",
            "data": {
                "user": CLIENT_NAME,
                "timestamp": time.time()
            }
        }
        rep = send_and_receive(socket, req)
        if rep.get("data", {}).get("status") == "sucesso":
            break
        time.sleep(2)


def list_channels(socket):
    req = {
        "service": "channels",
        "data": {
            "timestamp": time.time()
        }
    }
    rep = send_and_receive(socket, req)
    return rep.get("data", {}).get("channels", [])


def create_channel(socket, channel_name):
    req = {
        "service": "channel",
        "data": {
            "channel": channel_name,
            "timestamp": time.time()
        }
    }
    return send_and_receive(socket, req)


def main():
    time.sleep(2)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(BROKER_URL)

    print(f"[{CLIENT_NAME}] Bot iniciado e conectado em {BROKER_URL}", flush=True)

    # 1. Faz login assim que inicia
    do_login(socket)

    # 2. Fica listando e criando canais automaticamente
    while True:
        time.sleep(random.uniform(3, 6))
        existing_channels = list_channels(socket)

        candidate = random.choice(CHANNELS_POOL)
        if candidate not in existing_channels:
            time.sleep(1)
            create_channel(socket, candidate)


if __name__ == "__main__":
    main()