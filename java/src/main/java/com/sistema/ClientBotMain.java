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

    private static ZMQ.Socket subscriber = null;

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

            String subscriberAddress = getEnvVariable("SUBSCRIBER_ADDRESS", "localhost");
            log.info("Conectando no subscriber - " + "tcp://" + subscriberAddress + ":5558");
            subscriber = context.createSocket(ZMQ.SUB);

            login();
            Thread.sleep(2000);
            createChannel("futebol");
            Thread.sleep(2000);
            createChannel("comida");
            Thread.sleep(2000);
            createChannel("noticias");
            Thread.sleep(2000);
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

        if (channels == null || channels.isEmpty()) {
            log.info("Nenhum canal disponivel.");
            return;
        }

        int random = (int) (Math.random() * channels.size());
        String channelToSubscribe = channels.get(random);
        log.info("Canais disponiveis: `" + String.join("`, `", channels) + "`");
        log.info("Assinando canal: " + channelToSubscribe);
        subscriber.subscribe(channelToSubscribe.getBytes());
    }

    private static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }

    private static void startSubscriber(List<String> channels) {

        Thread subscriberThread = new Thread(() -> {

            try (ZContext context = new ZContext()) {

                ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);

                String subscriberAddress = getEnvVariable(
                        "SUBSCRIBER_ADDRESS", "localhost");

                subscriber.connect("tcp://" + subscriberAddress + ":5558");

                // Inscrições nos canais
                for (String channel : channels) {
                    subscriber.subscribe(channel.getBytes());
                    log.info("Inscrito no canal: " + channel);
                }

                // Recebimento das mensagens
                while (!Thread.currentThread().isInterrupted()) {

                    // Aqui ficará a lógica de recebimento

                }

            }
        });

        subscriberThread.setName("Subscriber-Thread");
        subscriberThread.start();
    }
}