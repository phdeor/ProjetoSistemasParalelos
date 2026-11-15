import os
import time
import json
import zmq
import msgpack
from datetime import datetime

SERVER_NAME = os.getenv("SERVER_NAME", "server_py_1")
BROKER_URL = os.getenv("BROKER_URL", "tcp://broker:5556")
PROXY_PUB_URL = os.getenv("PROXY_PUB_URL", "tcp://proxy_pubsub:5557")
PROXY_SUB_URL = os.getenv("PROXY_SUB_URL", "tcp://proxy_pubsub:5558")
DATA_DIR = os.getenv("DATA_DIR", "/data")
DB_FILE = os.path.join(DATA_DIR, "database.json")
# parte 3
REFERENCE_URL = os.getenv("REFERENCE_URL", "tcp://referencia:5559")

logical_clock = 0
clock_offset = 0.0
client_msg_count = 0
my_rank = 0
coordinator_name = None  # Variavel da Parte 4


def get_physical_time():
    return time.time() + clock_offset


def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def load_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DB_FILE):
        initial_data = {"logins": [], "channels": [], "messages": []}
        save_db(initial_data)
        return initial_data
    with open(DB_FILE, "r", encoding="utf-8") as f:
        db = json.load(f)
        if "messages" not in db:
            db["messages"] = []
        return db


def save_db(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def log_message(direction, msg_dict):
    ts = msg_dict.get("data", {}).get("timestamp", get_physical_time())
    ts_str = format_time(ts)
    lc = msg_dict.get("logical_clock", "?")
    print(f"[{SERVER_NAME}] [{ts_str}] [LC:{lc}] {direction}: {json.dumps(msg_dict, ensure_ascii=False)}", flush=True)


# Novas funcoes para a Parte 4 (Eleicao e Coordenador)
def start_election(context, ref_socket, pub_socket):
    global coordinator_name
    
    ref_socket.send(msgpack.packb({"service": "list"}, use_bin_type=True))
    rep = msgpack.unpackb(ref_socket.recv(), raw=False)
    servers = rep.get("servers", [])
    
    higher_ranks = [s["name"] for s in servers if s["rank"] > my_rank and s["name"] != SERVER_NAME]
    
    got_ok = False
    for peer in higher_ranks:
        url = f"tcp://{peer}:5560"
        req_elec = context.socket(zmq.REQ)
        req_elec.setsockopt(zmq.LINGER, 0)
        req_elec.connect(url)
        req_elec.send(msgpack.packb({"service": "eleicao"}, use_bin_type=True))
        
        poller = zmq.Poller()
        poller.register(req_elec, zmq.POLLIN)
        if dict(poller.poll(2000)):
            resp = msgpack.unpackb(req_elec.recv(), raw=False)
            if resp.get("status") == "OK":
                got_ok = True
        req_elec.close()
        
        if got_ok:
            break
            
    if not got_ok:
        coordinator_name = SERVER_NAME
        pub_socket.send_multipart([b"servers", SERVER_NAME.encode('utf-8')])


def sync_time_and_heartbeat(context, ref_socket, pub_socket):
    global clock_offset, coordinator_name
    
    req = {"service": "heartbeat", "name": SERVER_NAME}
    ref_socket.send(msgpack.packb(req, use_bin_type=True))
    ref_socket.recv() # O ref apenas responde OK agora

    if coordinator_name == SERVER_NAME:
        return # Sou o coordenador, nao peco a hora
        
    if coordinator_name:
        url = f"tcp://{coordinator_name}:5560"
        req_clock = context.socket(zmq.REQ)
        req_clock.setsockopt(zmq.LINGER, 0)
        req_clock.connect(url)
        req_clock.send(msgpack.packb({"service": "relogio"}, use_bin_type=True))
        
        poller = zmq.Poller()
        poller.register(req_clock, zmq.POLLIN)
        socks = dict(poller.poll(2000))
        
        if req_clock in socks:
            resp = msgpack.unpackb(req_clock.recv(), raw=False)
            if resp.get("status") == "ok":
                ref_time = resp.get("time")
                clock_offset = ref_time - time.time()
        else:
            coordinator_name = None
            start_election(context, ref_socket, pub_socket)
        req_clock.close()
    else:
        start_election(context, ref_socket, pub_socket)


def handle_request(request, pub_socket):
    global logical_clock
    service = request.get("service")
    data = request.get("data", {})
    db = load_db()

    current_time = get_physical_time()

    # 1. Login do usuário
    if service == "login":
        user = data.get("user", "").strip()
        req_ts = data.get("timestamp", current_time)

        if not user:
            return {
                "service": "login",
                "data": {
                    "status": "erro",
                    "timestamp": current_time,
                    "description": "Nome de usuario vazio."
                }
            }

        db["logins"].append({"user": user, "timestamp": req_ts})
        save_db(db)

        return {
            "service": "login",
            "data": {
                "status": "sucesso",
                "timestamp": current_time
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
                    "timestamp": current_time,
                    "description": "Nome de canal invalido."
                }
            }

        if channel in db["channels"]:
            return {
                "service": "channel",
                "data": {
                    "status": "erro",
                    "timestamp": current_time,
                    "description": f"Canal '{channel}' ja existe."
                }
            }

        db["channels"].append(channel)
        save_db(db)

        return {
            "service": "channel",
            "data": {
                "status": "sucesso",
                "timestamp": current_time
            }
        }

    # 3. Listagem de canais
    elif service == "channels":
        return {
            "service": "channels",
            "data": {
                "channels": db["channels"],
                "timestamp": current_time
            }
        }

    # 4. Publisher
    elif service == "publish":
        channel = data.get("channel", "").strip()
        msg_text = data.get("message", "").strip()
        req_ts = data.get("timestamp", current_time)
        
        if channel not in db["channels"]:
            return {"service": "publish", "data": {"status": "erro", "timestamp": current_time, "description": "Canal nao existe."}}
        
        db["messages"].append({"channel": channel, "message": msg_text, "timestamp": req_ts})
        save_db(db)
        
        logical_clock += 1
        pub_payload = {"channel": channel, "message": msg_text, "timestamp": req_ts, "logical_clock": logical_clock}
        packed_payload = msgpack.packb(pub_payload, use_bin_type=True)
        pub_socket.send_multipart([channel.encode('utf-8'), packed_payload])
        
        return {"service": "publish", "data": {"status": "sucesso", "timestamp": current_time}}

    else:
        return {
            "service": str(service),
            "data": {
                "status": "erro",
                "timestamp": current_time,
                "description": "Servico desconhecido."
            }
        }


def main():
    global logical_clock, clock_offset, client_msg_count, my_rank, coordinator_name
    context = zmq.Context()
    
    socket = context.socket(zmq.REP)
    socket.connect(BROKER_URL)
    
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PROXY_PUB_URL)
    
    # Novo socket ouvindo o pub/sub para receber o topico "servers"
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(PROXY_SUB_URL)
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "servers")
    
    ref_socket = context.socket(zmq.REQ)
    ref_socket.connect(REFERENCE_URL)

    # Novo socket para conversar com outros servidores (porta 5560)
    peer_socket = context.socket(zmq.REP)
    peer_socket.bind("tcp://*:5560")

    load_db()
    
    ref_socket.send(msgpack.packb({"service": "get_rank", "name": SERVER_NAME}, use_bin_type=True))
    rank_rep = msgpack.unpackb(ref_socket.recv(), raw=False)
    my_rank = rank_rep.get("rank")
    clock_offset = 0.0

    print(f"[{SERVER_NAME}] Conectado. Rank: {my_rank}.", flush=True)

    # Inicia a lógica da Parte 4 verificando eleicao
    start_election(context, ref_socket, pub_socket)

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(peer_socket, zmq.POLLIN)
    poller.register(sub_socket, zmq.POLLIN)

    while True:
        socks = dict(poller.poll(1000))
        
        # 1. Trata clientes
        if socket in socks:
            raw_req = socket.recv()
            request = msgpack.unpackb(raw_req, raw=False)
            
            recv_clock = request.get("logical_clock", 0)
            logical_clock = max(logical_clock, recv_clock)
            
            log_message("RECEBIDO", request)

            response = handle_request(request, pub_socket)

            logical_clock += 1
            response["logical_clock"] = logical_clock

            log_message("ENVIADO", response)
            raw_rep = msgpack.packb(response, use_bin_type=True)
            socket.send(raw_rep)

            client_msg_count += 1
            if client_msg_count >= 15: # Atualizado para 15 mensagens
                sync_time_and_heartbeat(context, ref_socket, pub_socket)
                client_msg_count = 0
                
        # 2. Trata comunicacao entre servidores (eleicao e pedido de relogio)
        if peer_socket in socks:
            raw_req = peer_socket.recv()
            req = msgpack.unpackb(raw_req, raw=False)
            service = req.get("service")
            
            if service == "eleicao":
                peer_socket.send(msgpack.packb({"status": "OK"}, use_bin_type=True))
                start_election(context, ref_socket, pub_socket)
            elif service == "relogio":
                peer_socket.send(msgpack.packb({"status": "ok", "time": get_physical_time()}, use_bin_type=True))

        # 3. Escuta quem venceu a eleicao no tópico
        if sub_socket in socks:
            topic, raw_payload = sub_socket.recv_multipart()
            if topic == b"servers":
                coordinator_name = raw_payload.decode('utf-8')
                print(f"[{SERVER_NAME}] Novo Coordenador eleito e reconhecido: {coordinator_name}", flush=True)


if __name__ == "__main__":
    main()