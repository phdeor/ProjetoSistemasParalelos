package com.sistema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sistema.model.ClientMessage;
import com.sistema.model.LoginPayload;
import com.sistema.model.ServerMessage;
import com.sistema.model.ServerStatusEnum;

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

            login();
            Thread.sleep(4000);
            createChannel("canal1");
            Thread.sleep(2000);
            createChannel("canal2");
            Thread.sleep(2000);
            createChannel("canal3");
            Thread.sleep(4000);
            listChannels();
        } catch (Exception e) {
            log.warn("Erro no cliente: " + e.getMessage());
        }
    }

    private static ServerMessage encodeMessage(String channel, JsonNode content) {
        try {
            ClientMessage msg = new ClientMessage(CLIENT_NAME, channel, content);
            socket.send(objectMapper.writeValueAsBytes(msg), 0);

            byte[] replyBytes = socket.recv(0);

            if (replyBytes != null) {
                ServerMessage respostaObj = objectMapper.readValue(replyBytes, ServerMessage.class);
                log.info("Resposta: " + respostaObj.getStatus() + " - " + respostaObj.getMessage());
                return respostaObj;
            }

            return null;
        } catch (IOException e) {
            log.warn("Erro ao codificar mensagem:" + e.getMessage());
        }
        return null;
    }

    private static void login() {
        log.info("Login do cliente " + CLIENT_NAME);
        List<LoginPayload> loginPayloads = Arrays.asList(
                new LoginPayload("user1", "password1"),
                new LoginPayload("user2", "password2"),
                new LoginPayload("admin", "admin"));

        for (LoginPayload loginPayload : loginPayloads) {
            var resposta = encodeMessage("login", objectMapper.valueToTree(loginPayload));
            if (resposta != null && resposta.getStatus() == ServerStatusEnum.OK) {
                break;
            }
        }
    }

    private static void createChannel(String channelName) {
        log.info("Criando canal: " + channelName);
        encodeMessage("channel", objectMapper.valueToTree(channelName));
    }

    private static void listChannels() {
        log.info("Listando canais disponiveis...");
        var resposta = encodeMessage("list_channels", null);
        List<String> channels = objectMapper.convertValue(
                resposta.getMessage(),
                new TypeReference<List<String>>() {
                });

        log.info(
                channels != null ? "Canais disponiveis: \n" + String.join("\n", channels) : "Nenhum canal disponivel.");
    }

    private static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }
}