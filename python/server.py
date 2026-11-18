import os
import time
import json
import random
import zmq
import msgpack
from datetime import datetime

SERVER_NAME = os.getenv("SERVER_NAME", "server_py_1")
BROKER_URL = os.getenv("BROKER_URL", "tcp://broker:5556")
PROXY_PUB_URL = os.getenv("PROXY_PUB_URL", "tcp://proxy_pubsub:5557")
PROXY_SUB_URL = os.getenv("PROXY_SUB_URL", "tcp://proxy_pubsub:5558")
DATA_DIR = os.getenv("DATA_DIR", "/data")
DB_FILE = os.path.join(DATA_DIR, "database.json")
REFERENCE_URL = os.getenv("REFERENCE_URL", "tcp://referencia:5559")

PEER_PORT = 5560
PEER_TIMEOUT_MS = 2000
SYNC_TIMEOUT_MS = 3000
HEARTBEAT_EVERY = 10   
CLOCK_SYNC_EVERY = 15  
REPL_TOPIC = b"replication"  

logical_clock = 0
clock_offset = 0.0
my_rank = 0
coordinator_name = None




def get_physical_time():
    return time.time() + clock_offset


def format_time(ts):
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def stamp(msg):
    global logical_clock
    logical_clock += 1
    msg["timestamp"] = get_physical_time()
    msg["logical_clock"] = logical_clock
    return msg


def observe(msg):
    global logical_clock
    logical_clock = max(logical_clock, msg.get("logical_clock", 0))


def summarize(msg):
    if isinstance(msg, dict) and isinstance(msg.get("ops"), list):
        msg = dict(msg)
        msg["ops"] = f"<{len(msg['ops'])} operacoes>"
    return msg


def log_message(direction, msg_dict):
    ts = msg_dict.get("data", {}).get("timestamp", msg_dict.get("timestamp", get_physical_time()))
    lc = msg_dict.get("logical_clock", "?")
    print(f"[{SERVER_NAME}] [{format_time(ts)}] [LC:{lc}] {direction}: {json.dumps(summarize(msg_dict), ensure_ascii=False)}", flush=True)




def ensure_schema(db):
    db.setdefault("logins", [])
    db.setdefault("channels", [])
    db.setdefault("messages", [])
    repl = db.setdefault("replication", {})
    repl.setdefault("seq", 0)      
    repl.setdefault("vector", {})  
    repl.setdefault("log", [])     
    return db


def load_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DB_FILE):
        db = ensure_schema({})
        save_db(db)
        return db
    with open(DB_FILE, "r", encoding="utf-8") as f:
        return ensure_schema(json.load(f))


def save_db(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)




def apply_op(db, op):
    op_type = op.get("type")
    data = op.get("data", {})
    origin = op.get("origin")

    if op_type == "login":
        db["logins"].append({"user": data.get("user"), "timestamp": data.get("timestamp"), "origin": origin})
    elif op_type == "channel":
        if data.get("channel") not in db["channels"]:
            db["channels"].append(data.get("channel"))
    elif op_type == "message":
        entry = dict(data)
        entry["origin"] = origin
        db["messages"].append(entry)


def commit_local(db, op_type, data):
    repl = db["replication"]
    repl["seq"] += 1
    op = stamp({"type": op_type, "origin": SERVER_NAME, "seq": repl["seq"], "data": data})
    apply_op(db, op)
    repl["log"].append(op)
    repl["vector"][SERVER_NAME] = repl["seq"]
    save_db(db)
    return op


def broadcast_op(pub_socket, op):
    log_message(f"REPLICANDO ({op['type']})", op)
    pub_socket.send_multipart([REPL_TOPIC, msgpack.packb(op, use_bin_type=True)])


def integrate_op(db, op):
    
    origin = op.get("origin")
    seq = op.get("seq", 0)
    if not origin:
        return "invalid"

    repl = db["replication"]
    last = repl["vector"].get(origin, 0)
    if seq <= last:
        return "dup"
    if seq != last + 1:
        return "gap"

    apply_op(db, op)
    repl["log"].append(op)
    repl["vector"][origin] = seq
    if origin == SERVER_NAME:
        repl["seq"] = seq  
    return "applied"


def receive_replicated_op(context, op):
    origin = op.get("origin")
    if origin == SERVER_NAME:
        return  

    log_message(f"RECEBIDO (replicacao de {origin})", op)
    db = load_db()
    status = integrate_op(db, op)

    if status == "applied":
        save_db(db)
    elif status == "gap":
        print(f"[{SERVER_NAME}] Operacao fora de ordem de {origin} (seq {op.get('seq')}). Sincronizando...", flush=True)
        sync_with_peer(context, origin)


def ops_after(vector):
    db = load_db()
    return [op for op in db["replication"]["log"] if op["seq"] > vector.get(op["origin"], 0)]


def sync_with_peer(context, peer):
    db = load_db()
    resp = peer_request(context, peer, "sync", {"vector": db["replication"]["vector"]}, SYNC_TIMEOUT_MS)
    if resp is None or str(resp.get("status")).upper() != "OK":
        return 0

    db = load_db()
    applied = 0
    for op in resp.get("ops", []):
        if integrate_op(db, op) == "applied":
            applied += 1
    if applied:
        save_db(db)
    print(f"[{SERVER_NAME}] Sincronizacao com {peer}: {applied} operacoes aplicadas", flush=True)
    return applied


def list_peers(ref_socket):
    rep = reference_request(ref_socket, {"service": "list"})
    return [s["name"] for s in rep.get("servers", []) if s["name"] != SERVER_NAME]


def initial_sync(context, ref_socket):
    for peer in list_peers(ref_socket):
        sync_with_peer(context, peer)


def reconcile(context, ref_socket):
    peers = list_peers(ref_socket)
    if peers:
        sync_with_peer(context, random.choice(peers))




def reference_request(ref_socket, msg):
    stamp(msg)
    log_message("ENVIADO (referencia)", msg)
    ref_socket.send(msgpack.packb(msg, use_bin_type=True))
    rep = msgpack.unpackb(ref_socket.recv(), raw=False)
    observe(rep)
    log_message("RECEBIDO (referencia)", rep)
    return rep


def send_heartbeat(ref_socket):
    reference_request(ref_socket, {"service": "heartbeat", "name": SERVER_NAME})




def peer_request(context, peer, service, extra=None, timeout_ms=PEER_TIMEOUT_MS):
    msg = {"service": service}
    if extra:
        msg.update(extra)
    req = stamp(msg)
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.connect(f"tcp://{peer}:{PEER_PORT}")
        log_message(f"ENVIADO (-> {peer})", req)
        sock.send(msgpack.packb(req, use_bin_type=True))
        if sock.poll(timeout_ms, zmq.POLLIN):
            resp = msgpack.unpackb(sock.recv(), raw=False)
            observe(resp)
            log_message(f"RECEBIDO (<- {peer})", resp)
            return resp
        return None
    except zmq.ZMQError as e:
        print(f"[{SERVER_NAME}] Sem comunicacao com {peer}: {e}", flush=True)
        return None
    finally:
        sock.close()


def announce_coordinator(pub_socket):
    payload = stamp({"coordinator": SERVER_NAME})
    log_message("PUBLICANDO no topico 'servers'", payload)
    pub_socket.send_multipart([b"servers", msgpack.packb(payload, use_bin_type=True)])


def start_election(context, ref_socket, pub_socket):
    global coordinator_name

    rep = reference_request(ref_socket, {"service": "list"})
    servers = rep.get("servers", [])
    higher_ranks = [s["name"] for s in servers if s["rank"] > my_rank and s["name"] != SERVER_NAME]
    print(f"[{SERVER_NAME}] Iniciando eleicao. Servidores com rank maior: {higher_ranks}", flush=True)

    got_ok = False
    for peer in higher_ranks:
        resp = peer_request(context, peer, "eleicao")
        if resp is not None and str(resp.get("status")).upper() == "OK":
            got_ok = True
            break

    if not got_ok:
        coordinator_name = SERVER_NAME
        print(f"[{SERVER_NAME}] Eleito como coordenador", flush=True)
        announce_coordinator(pub_socket)


def sync_clock(context, ref_socket, pub_socket):
    global clock_offset, coordinator_name

    if coordinator_name == SERVER_NAME:
        return  

    if not coordinator_name:
        start_election(context, ref_socket, pub_socket)
        return

    t0 = time.time()
    resp = peer_request(context, coordinator_name, "relogio")
    t1 = time.time()

    if resp is None:
        print(f"[{SERVER_NAME}] Coordenador {coordinator_name} indisponivel, iniciando eleicao", flush=True)
        coordinator_name = None
        start_election(context, ref_socket, pub_socket)
        return

    if str(resp.get("status")).upper() == "OK":
        clock_offset = resp["time"] + (t1 - t0) / 2 - t1
        print(f"[{SERVER_NAME}] Relogio sincronizado com {coordinator_name}. Ajuste: {clock_offset:.4f}s", flush=True)




def handle_request(request, pub_socket):
    global logical_clock
    service = request.get("service")
    data = request.get("data", {})
    db = load_db()

    current_time = get_physical_time()

    
    if service == "login":
        user = data.get("user", "").strip()
        req_ts = data.get("timestamp", current_time)

        if not user:
            return {
                "service": "login",
                "data": {"status": "erro", "timestamp": current_time, "description": "Nome de usuario vazio."}
            }

        op = commit_local(db, "login", {"user": user, "timestamp": req_ts})
        broadcast_op(pub_socket, op)

        return {"service": "login", "data": {"status": "sucesso", "timestamp": current_time}}

    
    elif service == "channel":
        channel = data.get("channel", "").strip()

        if not channel or " " in channel:
            return {
                "service": "channel",
                "data": {"status": "erro", "timestamp": current_time, "description": "Nome de canal invalido."}
            }

        if channel in db["channels"]:
            return {
                "service": "channel",
                "data": {"status": "erro", "timestamp": current_time, "description": f"Canal '{channel}' ja existe."}
            }

        op = commit_local(db, "channel", {"channel": channel})
        broadcast_op(pub_socket, op)

        return {"service": "channel", "data": {"status": "sucesso", "timestamp": current_time}}

    
    elif service == "channels":
        return {"service": "channels", "data": {"channels": db["channels"], "timestamp": current_time}}

    
    elif service == "publish":
        channel = data.get("channel", "").strip()
        msg_text = data.get("message", "").strip()
        user = data.get("user", "desconhecido")
        req_ts = data.get("timestamp", current_time)

        if channel not in db["channels"]:
            return {"service": "publish", "data": {"status": "erro", "timestamp": current_time, "description": "Canal nao existe."}}

        if not msg_text:
            return {"service": "publish", "data": {"status": "erro", "timestamp": current_time, "description": "Mensagem vazia."}}

        pub_ts = get_physical_time()  

        
        op = commit_local(db, "message", {
            "channel": channel,
            "message": msg_text,
            "user": user,
            "request_timestamp": req_ts,
            "timestamp": pub_ts
        })
        broadcast_op(pub_socket, op)

        
        
        logical_clock += 1
        pub_payload = {
            "channel": channel,
            "message": msg_text,
            "user": user,
            "timestamp": pub_ts,
            "logical_clock": logical_clock
        }
        log_message(f"PUBLICANDO no topico '{channel}'", pub_payload)
        pub_socket.send_multipart([channel.encode('utf-8'), msgpack.packb(pub_payload, use_bin_type=True)])

        return {"service": "publish", "data": {"status": "sucesso", "timestamp": current_time}}

    else:
        return {
            "service": str(service),
            "data": {"status": "erro", "timestamp": current_time, "description": "Servico desconhecido."}
        }


def handle_peer_request(peer_socket, context, ref_socket, pub_socket):
    req = msgpack.unpackb(peer_socket.recv(), raw=False)
    observe(req)
    log_message("RECEBIDO (peer)", req)
    service = req.get("service")

    if service == "eleicao":
        resp = {"status": "OK"}
    elif service == "relogio":
        resp = {"status": "OK", "time": get_physical_time()}
    elif service == "sync":
        resp = {"status": "OK", "ops": ops_after(req.get("vector", {}))}
    else:
        resp = {"status": "erro"}

    stamp(resp)
    log_message("ENVIADO (peer)", resp)
    peer_socket.send(msgpack.packb(resp, use_bin_type=True))

    
    if service == "eleicao":
        start_election(context, ref_socket, pub_socket)


def handle_subscription(sub_socket, context):
    global coordinator_name
    topic, raw_payload = sub_socket.recv_multipart()
    payload = msgpack.unpackb(raw_payload, raw=False)
    observe(payload)

    if topic == b"servers":
        log_message("RECEBIDO (topico 'servers')", payload)
        coordinator_name = payload.get("coordinator")
        print(f"[{SERVER_NAME}] Novo coordenador eleito e reconhecido: {coordinator_name}", flush=True)
    elif topic == REPL_TOPIC:
        receive_replicated_op(context, payload)


def main():
    global logical_clock, clock_offset, my_rank
    context = zmq.Context()

    socket = context.socket(zmq.REP)
    socket.connect(BROKER_URL)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(PROXY_PUB_URL)

    
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(PROXY_SUB_URL)
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "servers")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "replication")

    ref_socket = context.socket(zmq.REQ)
    ref_socket.connect(REFERENCE_URL)

    
    peer_socket = context.socket(zmq.REP)
    peer_socket.bind(f"tcp://*:{PEER_PORT}")

    load_db()

    
    
    time.sleep(1)

    rank_rep = reference_request(ref_socket, {"service": "get_rank", "name": SERVER_NAME})
    my_rank = rank_rep.get("rank")
    print(f"[{SERVER_NAME}] Conectado. Rank: {my_rank}.", flush=True)

    start_election(context, ref_socket, pub_socket)
    initial_sync(context, ref_socket)  

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(peer_socket, zmq.POLLIN)
    poller.register(sub_socket, zmq.POLLIN)

    msgs_since_heartbeat = 0
    msgs_since_sync = 0

    while True:
        socks = dict(poller.poll(1000))

        
        if socket in socks:
            request = msgpack.unpackb(socket.recv(), raw=False)
            observe(request)
            log_message("RECEBIDO", request)

            response = handle_request(request, pub_socket)

            logical_clock += 1
            response["logical_clock"] = logical_clock

            log_message("ENVIADO", response)
            socket.send(msgpack.packb(response, use_bin_type=True))

            msgs_since_heartbeat += 1
            msgs_since_sync += 1

            if msgs_since_heartbeat >= HEARTBEAT_EVERY:
                msgs_since_heartbeat = 0
                send_heartbeat(ref_socket)
                reconcile(context, ref_socket)

            if msgs_since_sync >= CLOCK_SYNC_EVERY:
                msgs_since_sync = 0
                sync_clock(context, ref_socket, pub_socket)

        
        if peer_socket in socks:
            handle_peer_request(peer_socket, context, ref_socket, pub_socket)

        
        if sub_socket in socks:
            handle_subscription(sub_socket, context)


if __name__ == "__main__":
    main()