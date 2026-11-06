package com.sistema;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ClientBotMain {
    private static final String CLIENT_NAME = getEnvVariable(
            "CLIENT_NAME", "JavaBot" + System.currentTimeMillis());

    private static final ObjectMapper objectMapper = new ObjectMapper(
            new MessagePackFactory());

    public static void main(String[] args) {

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(ZMQ.REQ);

            String serverAddress = getEnvVariable("SERVER_ADDRESS", "localhost");
            socket.connect("tcp://" + serverAddress + ":5555");

            // Agora o objectMapper é reconhecido aqui:
            byte[] payloadBinario = encodeMessage("canal1", "Olá do Java Bot!");
            socket.send(payloadBinario, 0);

            byte[] replyBytes = socket.recv(0);
            if (replyBytes != null) {
                // E aqui também:
                ClientMessage respostaObj = objectMapper.readValue(replyBytes, ClientMessage.class);
                System.out.println("[JAVA BOT] Resposta: " + respostaObj.getContent());
            }
        } catch (Exception e) {
            System.err.println("Erro no cliente: " + e.getMessage());
        }
    }

    private static byte[] encodeMessage(String channel, String content) {
        try {
            ClientMessage msg = new ClientMessage(CLIENT_NAME, channel, content);
            return objectMapper.writeValueAsBytes(msg);
        } catch (Exception e) {
            System.err.println("Erro ao codificar mensagem:" + e.getMessage());
            return null;
        }
    }

    private static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }
}