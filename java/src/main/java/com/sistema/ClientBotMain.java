package com.sistema;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ClientBotMain {
    private static final String CLIENT_NAME = getEnvVariable(
            "CLIENT_NAME", "JavaBot" + System.currentTimeMillis());

    private static ZMQ.Socket socket = null;

    private static final ObjectMapper objectMapper = new ObjectMapper(
            new MessagePackFactory());

    private static final Logger log = LoggerFactory.getLogger(ClientBotMain.class);

    public static void main(String[] args) {
        log.info("Iniciando client " + CLIENT_NAME);
        try (ZContext context = new ZContext()) {
            socket = context.createSocket(ZMQ.REQ);

            String serverAddress = getEnvVariable("SERVER_ADDRESS", "localhost");
            log.info("Conectando no broker - " + "tcp://" + serverAddress + ":5555");
            socket.connect("tcp://" + serverAddress + ":5555");

            encodeMessage("canal1", "Olá do Java Bot!");

            byte[] replyBytes = socket.recv(0);
            if (replyBytes != null) {
                ServerMessage respostaObj = objectMapper.readValue(replyBytes, ServerMessage.class);
                log.info("Resposta: " + respostaObj.getStatus() + " - " + respostaObj.getMessage());
            }
        } catch (Exception e) {
            log.warn("Erro no cliente: " + e.getMessage());
        }
    }

    private static void encodeMessage(String channel, String content) {
        try {
            ClientMessage msg = new ClientMessage(CLIENT_NAME, channel, content);
            socket.send(objectMapper.writeValueAsBytes(msg), 0);
        } catch (JsonProcessingException e) {
            log.warn("Erro ao codificar mensagem:" + e.getMessage());
        }
    }

    private static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }
}