package com.sistema;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ServerMain {
    private static final String SERVER_NAME = getEnvVariable(
            "SERVER_NAME", "ServidorJava" + System.currentTimeMillis());

    private static ZMQ.Socket socket = null;

    private static final ObjectMapper objectMapper = new ObjectMapper(
            new MessagePackFactory());

    private static final Logger log = LoggerFactory.getLogger(ServerMain.class);

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
                log.info("-> [RECEBIDO] " + mensagemRecebida.getContent());

                orchestrateMessage(mensagemRecebida);
            }
        } catch (Exception e) {
            log.warn("Erro no servidor: " + e.getMessage());
        }
    }

    private static void encodeMessageSucess(String message) {
        try {
            ServerMessage msg = new ServerMessage(SERVER_NAME, ServerStatusEnum.OK, message);
            socket.send(objectMapper.writeValueAsBytes(msg), 0);
        } catch (JsonProcessingException e) {
            log.warn("Erro ao codificar mensagem:" + e.getMessage());
        }
    }

    private static void encodeMessageError(String message) {
        try {
            ServerMessage msg = new ServerMessage(SERVER_NAME, ServerStatusEnum.ERROR, message);
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
        log.info("Orquestrando mensagem para o canal: " + mensagemRecebida.getChannel() + " com conteúdo: "
                + mensagemRecebida.getContent());

        if ("login".equalsIgnoreCase(mensagemRecebida.getChannel())) {
            handleLogin(mensagemRecebida);
        } else if ("channel".equalsIgnoreCase(mensagemRecebida.getChannel())) {
            createChannel(mensagemRecebida);
        } else if ("listar canais".equalsIgnoreCase(mensagemRecebida.getChannel())) {
            listChannels();
        } else {
            log.info("Canal não reconhecido: " + mensagemRecebida.getChannel());
            encodeMessageError("Canal não reconhecido: " + mensagemRecebida.getChannel());

        }
    }

    private static void handleLogin(ClientMessage mensagemRecebida) {
        log.info("Processando login para o usuário: " + mensagemRecebida.getSender());
        if ("simula_erro".equalsIgnoreCase(mensagemRecebida.getContent())) {
            log.warn("Falha no login para o usuário: " + mensagemRecebida.getSender());
            encodeMessageError("Falha no login para o usuário: " + mensagemRecebida.getSender());
        } else {
            log.info("Login bem-sucedido para o usuário: " + mensagemRecebida.getSender());
            encodeMessageSucess("Login bem-sucedido para o usuário: " + mensagemRecebida.getSender());
        }
    }

    private static void createChannel(ClientMessage mensagemRecebida) {
        log.info("Criando canal: " + mensagemRecebida.getContent());
        encodeMessageSucess("Canal criado com sucesso: " + mensagemRecebida.getContent());
    }

    private static void listChannels() {
        log.info("Listando canais disponíveis...");
        encodeMessageSucess("Canais disponíveis: canal1, canal2, canal3");
    }
}