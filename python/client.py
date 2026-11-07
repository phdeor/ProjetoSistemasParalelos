import os
import time
import json
import random
import zmq
import msgpack
from datetime import datetime

CLIENT_NAME = os.getenv("CLIENT_NAME", "bot_py_1")
BROKER_URL = os.getenv("BROKER_URL", "tcp://broker:5555")
PROXY_SUB_URL = os.getenv("PROXY_SUB_URL", "tcp://proxy_pubsub:5558")
CHANNELS_POOL = ["geral", "jogos", "avisos", "tecnologia", "musica"]

logical_clock = 0


def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def log_message(direction, msg_dict):
    ts = msg_dict.get("data", {}).get("timestamp", time.time())
    ts_str = format_time(ts)
    lc = msg_dict.get("logical_clock", "?")
    print(f"[{CLIENT_NAME}] [{ts_str}] [LC:{lc}] {direction}: {json.dumps(msg_dict, ensure_ascii=False)}", flush=True)


def send_and_receive(socket, payload):
    global logical_clock
    
    logical_clock += 1
    payload["logical_clock"] = logical_clock
    
    log_message("ENVIANDO", payload)
    packed = msgpack.packb(payload, use_bin_type=True)
    socket.send(packed)

    raw_reply = socket.recv()
    reply = msgpack.unpackb(raw_reply, raw=False)
    
    recv_clock = reply.get("logical_clock", 0)
    logical_clock = max(logical_clock, recv_clock)
    
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


def publish_message(req_socket, channel_name, msg_text):
    # Envia o pedido de publicação para o servidor
    req = {
        "service": "publish",
        "data": {
            "channel": channel_name,
            "message": msg_text,
            "timestamp": time.time()
        }
    }
    send_and_receive(req_socket, req)


def listen_sub_for(sub_socket, duration_seconds):
    global logical_clock
    # Fica ouvindo o proxy Pub/Sub
    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        try:
            topic, raw_payload = sub_socket.recv_multipart(flags=zmq.NOBLOCK)
            payload = msgpack.unpackb(raw_payload, raw=False)
            
            recv_clock = payload.get("logical_clock", 0)
            logical_clock = max(logical_clock, recv_clock)
            
            t_env = format_time(payload['timestamp'])
            t_rec = format_time(time.time())
            
            print(f"[{CLIENT_NAME}] *** MSG RECEBIDA *** -> Canal: {topic.decode('utf-8')} | Msg: {payload['message']} | Envio: {t_env} | Rec: {t_rec}", flush=True)
        except zmq.Again:
            time.sleep(0.1)


def main():
    time.sleep(2)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(BROKER_URL)
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(PROXY_SUB_URL)

    print(f"[{CLIENT_NAME}] Bot iniciado e conectado em {BROKER_URL}", flush=True)

    # 1. Faz login assim que inicia
    do_login(socket)
    subscribed_channels = []
    
    # 2. Fica listando e criando canais automaticamente
    while True:
        available_channels = list_channels(socket)

        # Regra 1: Se existirem menos do que 5 canais, criar um novo
        if len(available_channels) < 5:
            candidate = random.choice([c for c in CHANNELS_POOL if c not in available_channels])
            create_channel(socket, candidate)
            listen_sub_for(sub_socket, 1.0)
            continue
            
        # Regra 2: Se inscrito em menos do que 3 canais, se inscrever em mais um
        if len(subscribed_channels) < 3:
            unsubscribed = [c for c in available_channels if c not in subscribed_channels]
            if unsubscribed:
                ch = random.choice(unsubscribed)
                # O zmq.SUBSCRIBE é o comando que avisa o proxy: "Quero mensagens do canal X"
                sub_socket.setsockopt_string(zmq.SUBSCRIBE, ch)
                subscribed_channels.append(ch)
                print(f"[{CLIENT_NAME}] Assinou o canal: {ch}", flush=True)
            listen_sub_for(sub_socket, 1.0)
            continue
            
        # Regras 3, 4 e 5: Escolher um canal e enviar 10 mensagens
        target_channel = random.choice(available_channels)
        print(f"[{CLIENT_NAME}] Iniciando publicacao no canal '{target_channel}'...", flush=True)
        
        for i in range(1, 11):
            msg_text = f"Mensagem automatica {i} de {CLIENT_NAME}"
            publish_message(socket, target_channel, msg_text)
            
            # O intervalo de 1 segundo entre mensagens é feito ouvindo a rede
            listen_sub_for(sub_socket, 1.0)


if __name__ == "__main__":
    main()