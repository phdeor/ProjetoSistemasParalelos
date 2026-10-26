import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Cliente {

    public static void main(String[] args) {

        try (ZContext context = new ZContext()) {

            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            String broker = System.getenv()
                    .getOrDefault("BROKER_ADDRESS", "tcp://localhost:5555");

            socket.connect(broker);

            String mensagem = "Ola, servidor!";

            System.out.println("Enviando: " + mensagem);

            socket.send(mensagem);

            String resposta = socket.recvStr();

            System.out.println("Resposta recebida: " + resposta);

            socket.close();
        }
    }
}