import time
import json
import zmq
import msgpack

PORT = 5559

TIMEOUT = 60.0

logical_clock = 0


def stamp(msg):
    global logical_clock
    logical_clock += 1
    msg["timestamp"] = time.time()
    msg["logical_clock"] = logical_clock
    return msg


def observe(msg):
    global logical_clock
    logical_clock = max(logical_clock, msg.get("logical_clock", 0))


def log(direction, msg):
    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(msg.get("timestamp", time.time())))
    print(f"[REFERENCE] [{ts}] [LC:{msg.get('logical_clock', '?')}] {direction}: {json.dumps(msg, ensure_ascii=False)}", flush=True)


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{PORT}")

    servers = {}
    known_ranks = {}  
    next_rank = 1

    print(f"[REFERENCE] Servico de referencia iniciado na porta {PORT}...", flush=True)

    def register(name):
        nonlocal next_rank
        if name not in known_ranks:
            known_ranks[name] = next_rank
            next_rank += 1
        servers[name] = {"rank": known_ranks[name], "last_hb": time.time()}

    while True:
        if socket.poll(1000):
            req = msgpack.unpackb(socket.recv(), raw=False)
            observe(req)
            log("RECEBIDO", req)
            service = req.get("service")

            if service == "get_rank":
                name = req.get("name")
                register(name)
                rep = {"status": "ok", "rank": servers[name]["rank"]}
                print(f"[REFERENCE] Servidor '{name}' registrado. Rank: {servers[name]['rank']}", flush=True)

            elif service == "list":
                rep = {"status": "ok",
                       "servers": [{"name": k, "rank": v["rank"]} for k, v in servers.items()]}

            elif service == "heartbeat":
                name = req.get("name")
                if name in servers:
                    servers[name]["last_hb"] = time.time()
                elif name:
                    register(name)
                    print(f"[REFERENCE] Servidor '{name}' voltou a enviar heartbeat e foi re-registrado", flush=True)
                rep = {"status": "ok"}

            else:
                rep = {"status": "erro", "description": "Servico desconhecido."}

            stamp(rep)
            log("ENVIADO", rep)
            socket.send(msgpack.packb(rep, use_bin_type=True))

        now = time.time()
        for name in [n for n, d in servers.items() if now - d["last_hb"] > TIMEOUT]:
            print(f"[REFERENCE] Timeout! Removendo servidor inativo: {name}", flush=True)
            del servers[name]


if __name__ == "__main__":
    main()