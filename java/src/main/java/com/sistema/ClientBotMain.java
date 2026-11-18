package com.sistema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClientBotMain {
    private static final String CLIENT_NAME = env("CLIENT_NAME", "client_java");
    private static final String BROKER_URL = env("BROKER_URL", "tcp://broker:5555");
    private static final String PROXY_SUB_URL = env("PROXY_SUB_URL", "tcp://proxy_pubsub:5558");

    private static final List<String> CHANNELS_POOL = Arrays.asList(
            "geral", "jogos", "avisos", "tecnologia", "musica");

    private static final int MIN_CHANNELS = 5;
    private static final int MIN_SUBSCRIPTIONS = 3;
    private static final int MESSAGES_PER_ROUND = 10;

    private static final ObjectMapper msgpack = new ObjectMapper(new MessagePackFactory());
    private static final Logger log = LoggerFactory.getLogger(ClientBotMain.class);
    private static final Random random = new Random();
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private static ZMQ.Socket reqSocket;
    private static ZMQ.Socket subSocket;

    private static long logicalClock = 0;
    private static final List<String> subscribedChannels = new ArrayList<>();

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            Thread.sleep(2000);

            reqSocket = context.createSocket(ZMQ.REQ);
            reqSocket.connect(BROKER_URL);

            subSocket = context.createSocket(ZMQ.SUB);
            subSocket.connect(PROXY_SUB_URL);

            log.info("[{}] Bot iniciado e conectado em {}", CLIENT_NAME, BROKER_URL);

            doLogin();
            runBot();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Cliente interrompido", CLIENT_NAME);
        } catch (Exception e) {
            log.error("[" + CLIENT_NAME + "] Erro no cliente", e);
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }

    private static double nowSeconds() {
        return System.currentTimeMillis() / 1000.0;
    }

    private static String formatTime(double ts) {
        return TIME_FORMAT.format(Instant.ofEpochMilli((long) (ts * 1000)));
    }

    private static void logMessage(String direction, JsonNode msg) {
        double ts = msg.path("data").path("timestamp").asDouble(nowSeconds());
        log.info("[{}] [{}] [LC:{}] {}: {}", CLIENT_NAME, formatTime(ts),
                msg.path("logical_clock").asText("?"), direction, msg);
    }

    private static ObjectNode newRequest(String service, ObjectNode data) {
        data.put("timestamp", nowSeconds());
        ObjectNode msg = msgpack.createObjectNode();
        msg.put("service", service);
        msg.set("data", data);
        return msg;
    }

    private static JsonNode sendAndReceive(ObjectNode payload) throws IOException {
        logicalClock++;
        payload.put("logical_clock", logicalClock);

        logMessage("ENVIANDO", payload);
        reqSocket.send(msgpack.writeValueAsBytes(payload), 0);

        byte[] raw = reqSocket.recv(0);
        if (raw == null) {
            throw new IOException("Sem resposta do servidor");
        }
        JsonNode reply = msgpack.readTree(raw);
        logicalClock = Math.max(logicalClock, reply.path("logical_clock").asLong(0));

        logMessage("RECEBIDO", reply);
        return reply;
    }

    private static void doLogin() throws IOException, InterruptedException {
        while (true) {
            ObjectNode data = msgpack.createObjectNode();
            data.put("user", CLIENT_NAME);

            JsonNode rep = sendAndReceive(newRequest("login", data));
            if ("sucesso".equals(rep.path("data").path("status").asText())) {
                return;
            }
        }
    }

    private static List<String> listChannels() throws IOException {
        JsonNode rep = sendAndReceive(newRequest("channels", msgpack.createObjectNode()));
        List<String> channels = new ArrayList<>();
        for (JsonNode c : rep.path("data").path("channels")) {
            channels.add(c.asText());
        }
        return channels;
    }

    private static void createChannel(String channelName) throws IOException {
        ObjectNode data = msgpack.createObjectNode();
        data.put("channel", channelName);
        sendAndReceive(newRequest("channel", data));
    }

    private static void publishMessage(String channelName, String text) throws IOException {
        ObjectNode data = msgpack.createObjectNode();
        data.put("channel", channelName);
        data.put("message", text);
        data.put("user", CLIENT_NAME);
        sendAndReceive(newRequest("publish", data));
    }

    private static void runBot() throws IOException, InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            List<String> available = listChannels();

            if (available.size() < MIN_CHANNELS) {
                createChannel(pickNewChannelName(available));
                listenSubFor(1.0);
                continue;
            }

            if (subscribedChannels.size() < MIN_SUBSCRIPTIONS) {
                List<String> notSubscribed = new ArrayList<>(available);
                notSubscribed.removeAll(subscribedChannels);
                if (!notSubscribed.isEmpty()) {
                    String ch = notSubscribed.get(random.nextInt(notSubscribed.size()));
                    subSocket.subscribe(ch.getBytes(StandardCharsets.UTF_8));
                    subscribedChannels.add(ch);
                    log.info("[{}] Assinou o canal: {}", CLIENT_NAME, ch);
                }
                listenSubFor(1.0);
                continue;
            }

            String target = available.get(random.nextInt(available.size()));
            log.info("[{}] Iniciando publicacao no canal '{}'...", CLIENT_NAME, target);

            for (int i = 1; i <= MESSAGES_PER_ROUND; i++) {
                publishMessage(target, "Mensagem automatica " + i + " de " + CLIENT_NAME);

                listenSubFor(1.0);
            }
        }
    }

    private static String pickNewChannelName(List<String> available) {
        List<String> candidates = new ArrayList<>(CHANNELS_POOL);
        candidates.removeAll(available);
        if (candidates.isEmpty()) {
            return "canal_" + random.nextInt(100000);
        }
        return candidates.get(random.nextInt(candidates.size()));
    }

    private static void listenSubFor(double seconds) throws InterruptedException {
        long end = System.nanoTime() + (long) (seconds * 1_000_000_000L);

        while (System.nanoTime() < end) {
            byte[] topicBytes = subSocket.recv(ZMQ.DONTWAIT);
            if (topicBytes == null) {
                Thread.sleep(100);
                continue;
            }

            byte[] raw = subSocket.hasReceiveMore() ? subSocket.recv(0) : null;
            if (raw == null) {
                continue;
            }

            try {
                JsonNode payload = msgpack.readTree(raw);
                logicalClock = Math.max(logicalClock, payload.path("logical_clock").asLong(0));

                log.info("[{}] *** MSG RECEBIDA *** -> Canal: {} | Usuario: {} | Msg: {} | Envio: {} | Rec: {}",
                        CLIENT_NAME,
                        new String(topicBytes, StandardCharsets.UTF_8),
                        payload.path("user").asText("?"),
                        payload.path("message").asText(""),
                        formatTime(payload.path("timestamp").asDouble(nowSeconds())),
                        formatTime(nowSeconds()));
            } catch (IOException e) {
                log.warn("[{}] Publicacao invalida: {}", CLIENT_NAME, e.getMessage());
            }
        }
    }
}