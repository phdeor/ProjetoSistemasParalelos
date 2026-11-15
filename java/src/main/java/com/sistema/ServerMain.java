package com.sistema;

import java.util.ArrayList;
import java.util.List;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sistema.model.ClientMessage;
import com.sistema.model.LoginPayload;
import com.sistema.model.ServerMessage;
import com.sistema.model.ServerStatusEnum;

public class ServerMain {
    private static final String SERVER_NAME = getEnvVariable(
            "SERVER_NAME", "ServidorJava" + System.currentTimeMillis());

    private static ZMQ.Socket socket = null;

    private static final ObjectMapper objectMapper = new ObjectMapper(
            new MessagePackFactory());

    private static final Logger log = LoggerFactory.getLogger(ServerMain.class);

    private static final List<String> channels = new ArrayList<>();

    public static void main(String[] args) {
        log.info("Iniciando servidor: " + SERVER_NAME);

        try (ZContext context = new ZContext()) {
            socket = context.createSocket(ZMQ.REP);

            String serverAddress = getEnvVariable("SERVER_ADDRESS", "localhost");
            log.info("Iniciando servidor: " + "tcp://" + serverAddress + ":5556");
            socket.connect("tcp://" + serverAddress + ":5556");

            log.info("Aguardando mensagens...");

            while (!Thread.currentThread().isInterrupted()) {
                byte[] requestBytes = socket.recv(0);

                if (requestBytes == null)
                    break;

                ClientMessage mensagemRecebida = objectMapper.readValue(requestBytes, ClientMessage.class);
                log.info("[RECEBIDO] canal: " + mensagemRecebida.getChannel() + ", conteudo: "
                        + mensagemRecebida.getContent());

                orchestrateMessage(mensagemRecebida);
            }
        } catch (Exception e) {
            log.warn("Erro no servidor: " + e.getMessage());
        }
    }

    private static void encodeMessageSucess(JsonNode message) {
        try {
            ServerMessage msg = new ServerMessage(SERVER_NAME, ServerStatusEnum.OK, message);
            socket.send(objectMapper.writeValueAsBytes(msg), 0);
        } catch (JsonProcessingException e) {
            log.warn("Erro ao codificar mensagem:" + e.getMessage());
        }
    }

    private static void encodeMessageError(String message) {
        try {
            ServerMessage msg = new ServerMessage(SERVER_NAME, ServerStatusEnum.ERROR,
                    objectMapper.valueToTree(message));
            socket.send(objectMapper.writeValueAsBytes(msg), 0);
        } catch (JsonProcessingException e) {
            log.warn("Erro ao codificar mensagem:" + e.getMessage());
        }
    }

    private static String getEnvVariable(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }

    private static void orchestrateMessage(ClientMessage mensagemRecebida) {
        var channel = mensagemRecebida.getChannel();
        log.info("Orquestrando mensagem para o canal: " + channel + " com conteudo: "
                + mensagemRecebida.getContent());
        try {
            if ("login".equalsIgnoreCase(channel)) {
                handleLogin(mensagemRecebida);
            } else if ("channel".equalsIgnoreCase(channel)) {
                createChannel(mensagemRecebida);
            } else if ("list_channels".equalsIgnoreCase(channel)) {
                listChannels();
            } else {
                log.info("Canal nao reconhecido: " + channel);
                encodeMessageError("Canal nao reconhecido: " + channel);
            }

        } catch (Exception e) {
            log.warn("Erro ao processar mensagem: " + e.getMessage());
            encodeMessageError("Erro ao processar mensagem: " + e.getMessage());
        }
    }

    private static void handleLogin(ClientMessage mensagemRecebida) {
        log.info("Processando login para o usuario: " + mensagemRecebida.getSender());
        var content = mensagemRecebida.getContentAs(LoginPayload.class, objectMapper);
        if (content != null && "admin".equals(content.getUsername()) && "admin".equals(content.getPassword())) {
            log.info("Login bem-sucedido para o usuario: " + mensagemRecebida.getSender());
            encodeMessageSucess(
                    objectMapper.valueToTree("Login bem-sucedido para o usuario: " + mensagemRecebida.getSender()));
        } else {
            log.warn("Falha no login para o usuario: " + mensagemRecebida.getSender());
            encodeMessageError("Falha no login para o usuario: " + mensagemRecebida.getSender());
        }

    }

    private static void createChannel(ClientMessage mensagemRecebida) {
        var content = mensagemRecebida.getContentAs(String.class, objectMapper);
        log.info("Criando canal: " + content);
        if (content != null && !content.isEmpty() && !channels.contains(content)) {
            channels.add(content);
            log.info("Canal criado com sucesso: " + content);
            encodeMessageSucess(objectMapper.valueToTree("Canal criado com sucesso: " + content));
        } else {
            log.warn("Falha ao criar canal: " + content);
            encodeMessageError("Falha ao criar canal: " + content);
        }
    }

    private static void listChannels() {
        log.info("Listando canais disponiveis...");
        encodeMessageSucess(objectMapper.valueToTree(channels));
    }
}