import zmq

def main():
    context = zmq.Context()

    # XSUB: Onde os Servidores conectam para publicar mensagens (PUB -> XSUB)
    frontend = context.socket(zmq.XSUB)
    frontend.bind("tcp://*:5557")

    # XPUB: Onde os Clientes conectam para ouvir mensagens (SUB -> XPUB)
    backend = context.socket(zmq.XPUB)
    backend.bind("tcp://*:5558")

    print("[PROXY PUB/SUB] Iniciado nas portas 5557 (XSUB) e 5558 (XPUB)...", flush=True)
    
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        pass
    finally:
        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    main()