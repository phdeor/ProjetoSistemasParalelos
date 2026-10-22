import os
import time
import json
import zmq
import msgpack
from datetime import datetime

SERVER_NAME = os.getenv("SERVER_NAME", "server_py_1")
BROKER_URL = os.getenv("BROKER_URL", "tcp://broker:5556")
DATA_DIR = os.getenv("DATA_DIR", "/data")
DB_FILE = os.path.join(DATA_DIR, "database.json")


def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def load_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DB_FILE):
        initial_data = {"logins": [], "channels": []}
        save_db(initial_data)
        return initial_data
    with open(DB_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_db(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def log_message(direction, msg_dict):
    ts = msg_dict.get("data", {}).get("timestamp", time.time())
    ts_str = format_time(ts)
    print(f"[{SERVER_NAME}] [{ts_str}] {direction}: {json.dumps(msg_dict, ensure_ascii=False)}", flush=True)


def handle_request(request):
    service = request.get("service")
    data = request.get("data", {})
    db = load_db()

    # 1. Login do usuário
    if service == "login":
        user = data.get("user", "").strip()
        req_ts = data.get("timestamp", time.time())

        if not user:
            return {
                "service": "login",
                "data": {
                    "status": "erro",
                    "timestamp": time.time(),
                    "description": "Nome de usuario vazio."
                }
            }

        db["logins"].append({"user": user, "timestamp": req_ts})
        save_db(db)

        return {
            "service": "login",
            "data": {
                "status": "sucesso",
                "timestamp": time.time()
            }
        }

    # 2. Criação de canal
    elif service == "channel":
        channel = data.get("channel", "").strip()

        if not channel or " " in channel:
            return {
                "service": "channel",
                "data": {
                    "status": "erro",
                    "timestamp": time.time(),
                    "description": "Nome de canal invalido."
                }
            }

        if channel in db["channels"]:
            return {
                "service": "channel",
                "data": {
                    "status": "erro",
                    "timestamp": time.time(),
                    "description": f"Canal '{channel}' ja existe."
                }
            }

        db["channels"].append(channel)
        save_db(db)

        return {
            "service": "channel",
            "data": {
                "status": "sucesso",
                "timestamp": time.time()
            }
        }

    # 3. Listagem de canais
    elif service == "channels":
        return {
            "service": "channels",
            "data": {
                "channels": db["channels"],
                "timestamp": time.time()
            }
        }

    else:
        return {
            "service": str(service),
            "data": {
                "status": "erro",
                "timestamp": time.time(),
                "description": "Servico desconhecido."
            }
        }


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect(BROKER_URL)

    load_db()
    print(f"[{SERVER_NAME}] Conectado em {BROKER_URL}. Aguardando mensagens...", flush=True)

    while True:
        # Recebe binário (MessagePack) e converte para dicionário
        raw_req = socket.recv()
        request = msgpack.unpackb(raw_req, raw=False)
        log_message("RECEBIDO", request)

        response = handle_request(request)

        # Converte resposta para binário (MessagePack) e envia
        log_message("ENVIADO", response)
        raw_rep = msgpack.packb(response, use_bin_type=True)
        socket.send(raw_rep)


if __name__ == "__main__":
    main()