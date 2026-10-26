
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Servidor {

    public static void main(String[] args) {

        try (ZContext context = new ZContext()) {

            ZMQ.Socket socket = context.createSocket(SocketType.REP);

            String broker = System.getenv()
                    .getOrDefault("BROKER_ADDRESS", "tcp://localhost:5556");

            socket.connect(broker);

            System.out.println("Servidor conectado ao broker.");
            System.out.println("Aguardando mensagens...");

            while (!Thread.currentThread().isInterrupted()) {

                String mensagem = socket.recvStr();

                if (mensagem == null) {
                    break;
                }

                System.out.println("Mensagem recebida: " + mensagem);

                String resposta = "Servidor Java recebeu: " + mensagem;

                socket.send(resposta);

                System.out.println("Resposta enviada: " + resposta);
            }

            socket.close();
        }
    }
}