package com.sistema;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ServerMain {
    private static final String SERVER_NAME = getEnvVariable(
            "SERVER_NAME", "ServidorJava" + System.currentTimeMillis());

    private static final ObjectMapper objectMapper = new ObjectMapper(
            new MessagePackFactory());

    public static void main(String[] args) {

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(ZMQ.REP);

            String serverAddress = getEnvVariable("SERVER_ADDRESS", "localhost");
            socket.connect("tcp://" + serverAddress + ":5556");

            System.out.println("[JAVA SERVER] Aguardando mensagens...");

            while (!Thread.currentThread().isInterrupted()) {
                byte[] requestBytes = socket.recv(0);

                if (requestBytes == null)
                    break;

                ClientMessage mensagemRecebida = objectMapper.readValue(requestBytes, ClientMessage.class);
                System.out.println("-> [RECEBIDO] " + mensagemRecebida.getContent());

                byte[] responseBytes = encodeMessageSucess("Mensagem recebida com sucesso!");
                socket.send(responseBytes, 0);
            }
        } catch (Exception e) {
            System.err.println("Erro no servidor: " + e.getMessage());
        }
    }

    private static byte[] encodeMessageSucess(String message) {
        try {
            ServerMessage msg = new ServerMessage(SERVER_NAME, ServerStatusEnum.OK, message);
            return objectMapper.writeValueAsBytes(msg);
        } catch (JsonProcessingException e) {
            System.err.println("Erro ao codificar mensagem:" + e.getMessage());
            return null;
        }
    }

    private static byte[] encodeMessageError(String message) {
        try {
            ServerMessage msg = new ServerMessage(SERVER_NAME, ServerStatusEnum.ERROR, message);
            return objectMapper.writeValueAsBytes(msg);
        } catch (JsonProcessingException e) {
            System.err.println("Erro ao codificar mensagem:" + e.getMessage());
            return null;
        }
    }

    private static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }
}