import zmq
import time
import msgpack

def main():
    context = zmq.Context()
    

    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5559")
    
    # Dicionário de servidores e tempo limite de 20s para o heartbeat
    servers = {}
    next_rank = 1
    TIMEOUT = 20.0  

    print("[REFERENCE] Coordenador iniciado na porta 5559...", flush=True)

    while True:
        try:
    
            raw_req = socket.recv(flags=zmq.NOBLOCK)
            req = msgpack.unpackb(raw_req, raw=False)
            service = req.get("service")
            

            if service == "get_rank":
                name = req.get("name")
                if name not in servers:
                    servers[name] = {"rank": next_rank, "last_hb": time.time()}
                    next_rank += 1
                else:
                    servers[name]["last_hb"] = time.time()
                rep = {"status": "ok", "rank": servers[name]["rank"], "time": time.time()}
                socket.send(msgpack.packb(rep, use_bin_type=True))
                print(f"[REFERENCE] Servidor '{name}' registrou. Rank: {servers[name]['rank']}.", flush=True)
            elif service == "list":
                active_servers = [{"name": k, "rank": v["rank"]} for k, v in servers.items()]
                rep = {"status": "ok", "servers": active_servers}
                socket.send(msgpack.packb(rep, use_bin_type=True))
            elif service == "heartbeat":
                name = req.get("name")
                if name in servers:
                    servers[name]["last_hb"] = time.time()
                
                rep = {"status": "ok", "time": time.time()}
                socket.send(msgpack.packb(rep, use_bin_type=True))
                
        except zmq.Again:
            time.sleep(0.1) 
        current_time = time.time()
        to_remove = [name for name, data in servers.items() if (current_time - data["last_hb"]) > TIMEOUT]
        
        for name in to_remove:
            print(f"[REFERENCE] Timeout! Removendo servidor inativo: {name}", flush=True)
            del servers[name]

if __name__ == "__main__":
    main()