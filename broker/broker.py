import zmq

def main():
    context = zmq.Context()

    # Porta onde os clientes (bots) vão ligar
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")

    # Porta onde os servidores vão ligar
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")

    print("[BROKER] Iniciado nas portas 5555 (clientes) e 5556 (servidores)...", flush=True)
    zmq.proxy(frontend, backend)

if __name__ == "__main__":
    main()