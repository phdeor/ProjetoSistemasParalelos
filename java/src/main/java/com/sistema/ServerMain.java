package com.sistema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ServerMain {
    private static final String SERVER_NAME = env("SERVER_NAME", "server_java_1");
    private static final String BROKER_URL = env("BROKER_URL", "tcp://broker:5556");
    private static final String PROXY_PUB_URL = env("PROXY_PUB_URL", "tcp://proxy_pubsub:5557");
    private static final String PROXY_SUB_URL = env("PROXY_SUB_URL", "tcp://proxy_pubsub:5558");
    private static final String REFERENCE_URL = env("REFERENCE_URL", "tcp://referencia:5559");
    private static final String DATA_DIR = env("DATA_DIR", "/data");

    private static final int PEER_PORT = 5560;
    private static final int PEER_TIMEOUT_MS = 2000;
    private static final int SYNC_TIMEOUT_MS = 3000;
    private static final int HEARTBEAT_EVERY = 10;
    private static final int CLOCK_SYNC_EVERY = 15;
    private static final String REPL_TOPIC = "replication";

    private static final ObjectMapper msgpack = new ObjectMapper(new MessagePackFactory());
    private static final ObjectMapper json = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ServerMain.class);
    private static final Random random = new Random();
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private static ZContext context;
    private static ZMQ.Socket repSocket;
    private static ZMQ.Socket pubSocket;
    private static ZMQ.Socket subSocket;
    private static ZMQ.Socket refSocket;
    private static ZMQ.Socket peerSocket;

    private static long logicalClock = 0;
    private static double clockOffset = 0.0;
    private static int myRank = 0;
    private static String coordinatorName = null;
    private static int msgsSinceHeartbeat = 0;
    private static int msgsSinceSync = 0;

    private static ObjectNode db;

    public static void main(String[] args) {
        log.info("[{}] Iniciando servidor", SERVER_NAME);

        try (ZContext ctx = new ZContext()) {
            context = ctx;

            repSocket = ctx.createSocket(ZMQ.REP);
            repSocket.connect(BROKER_URL);

            pubSocket = ctx.createSocket(ZMQ.PUB);
            pubSocket.connect(PROXY_PUB_URL);

            subSocket = ctx.createSocket(ZMQ.SUB);
            subSocket.connect(PROXY_SUB_URL);
            subSocket.subscribe("servers".getBytes(StandardCharsets.UTF_8));
            subSocket.subscribe(REPL_TOPIC.getBytes(StandardCharsets.UTF_8));

            refSocket = ctx.createSocket(ZMQ.REQ);
            refSocket.connect(REFERENCE_URL);

            peerSocket = ctx.createSocket(ZMQ.REP);
            peerSocket.bind("tcp://*:" + PEER_PORT);

            db = loadDb();

            Thread.sleep(1000);

            myRank = requestRank();
            log.info("[{}] Conectado. Rank: {}", SERVER_NAME, myRank);

            startElection();
            initialSync();

            ZMQ.Poller poller = ctx.createPoller(3);
            poller.register(repSocket, ZMQ.Poller.POLLIN);
            poller.register(peerSocket, ZMQ.Poller.POLLIN);
            poller.register(subSocket, ZMQ.Poller.POLLIN);

            while (!Thread.currentThread().isInterrupted()) {
                poller.poll(1000);

                if (poller.pollin(0)) {
                    handleClientRequest();
                }
                if (poller.pollin(1)) {
                    handlePeerRequest();
                }
                if (poller.pollin(2)) {
                    handleSubscription();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Servidor interrompido", SERVER_NAME);
        } catch (Exception e) {
            log.error("[" + SERVER_NAME + "] Erro no servidor", e);
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null) ? value : defaultValue;
    }

    private static double now() {
        return System.currentTimeMillis() / 1000.0 + clockOffset;
    }

    private static String formatTime(double ts) {
        return TIME_FORMAT.format(Instant.ofEpochMilli((long) (ts * 1000)));
    }

    private static ObjectNode stamp(ObjectNode msg) {
        logicalClock++;
        msg.put("timestamp", now());
        msg.put("logical_clock", logicalClock);
        return msg;
    }

    private static void observe(JsonNode msg) {
        logicalClock = Math.max(logicalClock, msg.path("logical_clock").asLong(0));
    }

    private static void logMessage(String direction, JsonNode msg) {
        double ts = msg.path("data").path("timestamp").asDouble(msg.path("timestamp").asDouble(now()));
        JsonNode shown = msg;
        if (msg.path("ops").isArray()) {
            ObjectNode copy = msg.deepCopy();
            copy.put("ops", "<" + msg.path("ops").size() + " operacoes>");
            shown = copy;
        }
        log.info("[{}] [{}] [LC:{}] {}: {}", SERVER_NAME, formatTime(ts),
                msg.path("logical_clock").asText("?"), direction, shown);
    }

    private static ObjectNode newRequest(String service) {
        ObjectNode msg = msgpack.createObjectNode();
        msg.put("service", service);
        return msg;
    }

    private static ObjectNode reply(String service, String status, String description) {
        ObjectNode data = msgpack.createObjectNode();
        data.put("status", status);
        data.put("timestamp", now());
        if (description != null) {
            data.put("description", description);
        }
        ObjectNode resp = msgpack.createObjectNode();
        resp.put("service", service);
        resp.set("data", data);
        return resp;
    }

    private static File dbFile() {
        return new File(DATA_DIR, "database.json");
    }

    private static ObjectNode loadDb() throws IOException {
        new File(DATA_DIR).mkdirs();
        ObjectNode loaded;
        if (dbFile().exists()) {
            loaded = (ObjectNode) json.readTree(dbFile());
        } else {
            loaded = json.createObjectNode();
        }
        for (String key : new String[] { "logins", "channels", "messages" }) {
            if (!loaded.has(key)) {
                loaded.putArray(key);
            }
        }
        if (!loaded.has("replication")) {
            loaded.putObject("replication");
        }
        ObjectNode repl = (ObjectNode) loaded.get("replication");
        if (!repl.has("seq")) {
            repl.put("seq", 0L);
        }
        if (!repl.has("vector")) {
            repl.putObject("vector");
        }
        if (!repl.has("log")) {
            repl.putArray("log");
        }
        db = loaded;
        saveDb();
        return loaded;
    }

    private static void saveDb() throws IOException {
        new File(DATA_DIR).mkdirs();
        json.writerWithDefaultPrettyPrinter().writeValue(dbFile(), db);
    }

    private static ArrayNode table(String name) {
        return (ArrayNode) db.get(name);
    }

    private static ObjectNode repl() {
        return (ObjectNode) db.get("replication");
    }

    private static ObjectNode replVector() {
        return (ObjectNode) repl().get("vector");
    }

    private static ArrayNode replLog() {
        return (ArrayNode) repl().get("log");
    }

    private static boolean channelExists(String channel) {
        for (JsonNode c : table("channels")) {
            if (c.asText().equals(channel)) {
                return true;
            }
        }
        return false;
    }

    private static void applyOp(JsonNode op) {
        String type = op.path("type").asText("");
        JsonNode data = op.path("data");
        String origin = op.path("origin").asText("");

        switch (type) {
            case "login": {
                ObjectNode entry = json.createObjectNode();
                entry.put("user", data.path("user").asText(""));
                entry.put("timestamp", data.path("timestamp").asDouble(0));
                entry.put("origin", origin);
                table("logins").add(entry);
                break;
            }
            case "channel": {
                String channel = data.path("channel").asText("");
                if (!channel.isEmpty() && !channelExists(channel)) {
                    table("channels").add(channel);
                }
                break;
            }
            case "message": {
                ObjectNode entry = data.deepCopy();
                entry.put("origin", origin);
                table("messages").add(entry);
                break;
            }
            default:
                log.warn("[{}] Tipo de operacao desconhecido: {}", SERVER_NAME, type);
        }
    }

    private static ObjectNode commitLocal(String type, ObjectNode data) throws IOException {
        long seq = repl().path("seq").asLong(0) + 1;
        repl().put("seq", seq);

        ObjectNode op = msgpack.createObjectNode();
        op.put("type", type);
        op.put("origin", SERVER_NAME);
        op.put("seq", seq);
        op.set("data", data);
        stamp(op);

        applyOp(op);
        replLog().add(op);
        replVector().put(SERVER_NAME, seq);
        saveDb();
        return op;
    }

    private static void broadcastOp(ObjectNode op) throws IOException {
        logMessage("REPLICANDO (" + op.path("type").asText() + ")", op);
        pubSocket.sendMore(REPL_TOPIC.getBytes(StandardCharsets.UTF_8));
        pubSocket.send(msgpack.writeValueAsBytes(op), 0);
    }

    private static String integrateOp(JsonNode op) {
        String origin = op.path("origin").asText("");
        long seq = op.path("seq").asLong(0);
        if (origin.isEmpty()) {
            return "invalid";
        }

        long last = replVector().path(origin).asLong(0);
        if (seq <= last) {
            return "dup";
        }
        if (seq != last + 1) {
            return "gap";
        }

        applyOp(op);
        JsonNode copy = op.deepCopy();
        replLog().add(copy);
        replVector().put(origin, seq);
        if (origin.equals(SERVER_NAME)) {
            repl().put("seq", seq);
        }
        return "applied";
    }

    private static void receiveReplicatedOp(JsonNode op) throws IOException {
        String origin = op.path("origin").asText("");
        if (origin.equals(SERVER_NAME)) {
            return;
        }

        logMessage("RECEBIDO (replicacao de " + origin + ")", op);
        String status = integrateOp(op);

        if ("applied".equals(status)) {
            saveDb();
        } else if ("gap".equals(status)) {
            log.info("[{}] Operacao fora de ordem de {} (seq {}). Sincronizando...", SERVER_NAME, origin,
                    op.path("seq").asLong());
            syncWithPeer(origin);
        }
    }

    private static ArrayNode opsAfter(JsonNode vector) {
        ArrayNode out = msgpack.createArrayNode();
        for (JsonNode op : replLog()) {
            long known = vector.path(op.path("origin").asText("")).asLong(0);
            if (op.path("seq").asLong(0) > known) {
                out.add(op);
            }
        }
        return out;
    }

    private static int syncWithPeer(String peer) throws IOException {
        ObjectNode extra = msgpack.createObjectNode();
        JsonNode vectorCopy = replVector().deepCopy();
        extra.set("vector", vectorCopy);

        JsonNode resp = peerRequest(peer, "sync", extra, SYNC_TIMEOUT_MS);
        if (resp == null || !"OK".equalsIgnoreCase(resp.path("status").asText(""))) {
            return 0;
        }

        int applied = 0;
        for (JsonNode op : resp.path("ops")) {
            if ("applied".equals(integrateOp(op))) {
                applied++;
            }
        }
        if (applied > 0) {
            saveDb();
        }
        log.info("[{}] Sincronizacao com {}: {} operacoes aplicadas", SERVER_NAME, peer, applied);
        return applied;
    }

    private static List<String> otherServers() throws IOException {
        JsonNode rep = referenceRequest(newRequest("list"));
        List<String> names = new ArrayList<>();
        for (JsonNode s : rep.path("servers")) {
            String name = s.path("name").asText("");
            if (!name.isEmpty() && !name.equals(SERVER_NAME)) {
                names.add(name);
            }
        }
        return names;
    }

    private static void initialSync() throws IOException {
        for (String peer : otherServers()) {
            syncWithPeer(peer);
        }
    }

    private static void reconcile() throws IOException {
        List<String> peers = otherServers();
        if (!peers.isEmpty()) {
            syncWithPeer(peers.get(random.nextInt(peers.size())));
        }
    }

    private static void handleClientRequest() {
        byte[] raw = repSocket.recv(0);
        if (raw == null) {
            return;
        }

        ObjectNode response;
        try {
            JsonNode request = msgpack.readTree(raw);
            observe(request);
            logMessage("RECEBIDO", request);
            response = handleRequest(request);
        } catch (Exception e) {
            log.warn("[{}] Erro ao processar mensagem: {}", SERVER_NAME, e.getMessage());
            response = reply("erro", "erro", "Mensagem invalida: " + e.getMessage());
        }

        logicalClock++;
        response.put("logical_clock", logicalClock);
        logMessage("ENVIADO", response);

        // REP precisa sempre responder, senao o socket trava
        try {
            repSocket.send(msgpack.writeValueAsBytes(response), 0);
        } catch (IOException e) {
            log.warn("[{}] Erro ao serializar resposta: {}", SERVER_NAME, e.getMessage());
            repSocket.send(new byte[0], 0);
        }

        msgsSinceHeartbeat++;
        msgsSinceSync++;

        if (msgsSinceHeartbeat >= HEARTBEAT_EVERY) {
            msgsSinceHeartbeat = 0;
            try {
                sendHeartbeat();
                reconcile();
            } catch (Exception e) {
                log.warn("[{}] Erro no heartbeat/reconciliacao: {}", SERVER_NAME, e.getMessage());
            }
        }
        if (msgsSinceSync >= CLOCK_SYNC_EVERY) {
            msgsSinceSync = 0;
            try {
                syncClock();
            } catch (Exception e) {
                log.warn("[{}] Erro na sincronizacao do relogio: {}", SERVER_NAME, e.getMessage());
            }
        }
    }

    private static ObjectNode handleRequest(JsonNode request) throws IOException {
        String service = request.path("service").asText("");
        JsonNode data = request.path("data");

        switch (service) {
            case "login":
                return handleLogin(data);
            case "channel":
                return handleCreateChannel(data);
            case "channels":
                return handleListChannels();
            case "publish":
                return handlePublish(data);
            default:
                return reply(service, "erro", "Servico desconhecido.");
        }
    }

    private static ObjectNode handleLogin(JsonNode data) throws IOException {
        String user = data.path("user").asText("").trim();
        double reqTs = data.path("timestamp").asDouble(now());

        if (user.isEmpty()) {
            return reply("login", "erro", "Nome de usuario vazio.");
        }

        ObjectNode opData = json.createObjectNode();
        opData.put("user", user);
        opData.put("timestamp", reqTs);
        broadcastOp(commitLocal("login", opData));

        return reply("login", "sucesso", null);
    }

    private static ObjectNode handleCreateChannel(JsonNode data) throws IOException {
        String channel = data.path("channel").asText("").trim();

        if (channel.isEmpty() || channel.contains(" ")) {
            return reply("channel", "erro", "Nome de canal invalido.");
        }
        if (channelExists(channel)) {
            return reply("channel", "erro", "Canal '" + channel + "' ja existe.");
        }

        ObjectNode opData = json.createObjectNode();
        opData.put("channel", channel);
        broadcastOp(commitLocal("channel", opData));

        return reply("channel", "sucesso", null);
    }

    private static ObjectNode handleListChannels() {
        ObjectNode resp = reply("channels", "sucesso", null);
        ObjectNode data = (ObjectNode) resp.get("data");
        data.remove("status");
        JsonNode channelsCopy = table("channels").deepCopy();
        data.set("channels", channelsCopy);
        return resp;
    }

    private static ObjectNode handlePublish(JsonNode data) throws IOException {
        String channel = data.path("channel").asText("").trim();
        String message = data.path("message").asText("").trim();
        String user = data.path("user").asText("desconhecido");
        double reqTs = data.path("timestamp").asDouble(now());

        if (!channelExists(channel)) {
            return reply("publish", "erro", "Canal nao existe.");
        }
        if (message.isEmpty()) {
            return reply("publish", "erro", "Mensagem vazia.");
        }

        double pubTs = now();

        ObjectNode opData = json.createObjectNode();
        opData.put("channel", channel);
        opData.put("message", message);
        opData.put("user", user);
        opData.put("request_timestamp", reqTs);
        opData.put("timestamp", pubTs);
        broadcastOp(commitLocal("message", opData));

        logicalClock++;
        ObjectNode payload = msgpack.createObjectNode();
        payload.put("channel", channel);
        payload.put("message", message);
        payload.put("user", user);
        payload.put("timestamp", pubTs);
        payload.put("logical_clock", logicalClock);

        logMessage("PUBLICANDO no topico '" + channel + "'", payload);
        pubSocket.sendMore(channel.getBytes(StandardCharsets.UTF_8));
        pubSocket.send(msgpack.writeValueAsBytes(payload), 0);

        return reply("publish", "sucesso", null);
    }

    private static JsonNode referenceRequest(ObjectNode msg) throws IOException {
        stamp(msg);
        logMessage("ENVIADO (referencia)", msg);
        refSocket.send(msgpack.writeValueAsBytes(msg), 0);

        byte[] raw = refSocket.recv(0);
        if (raw == null) {
            throw new IOException("Sem resposta do servico de referencia");
        }
        JsonNode rep = msgpack.readTree(raw);
        observe(rep);
        logMessage("RECEBIDO (referencia)", rep);
        return rep;
    }

    private static int requestRank() throws IOException {
        ObjectNode req = newRequest("get_rank");
        req.put("name", SERVER_NAME);
        return referenceRequest(req).path("rank").asInt(0);
    }

    private static void sendHeartbeat() throws IOException {
        ObjectNode req = newRequest("heartbeat");
        req.put("name", SERVER_NAME);
        referenceRequest(req);
    }

    private static JsonNode peerRequest(String peer, String service) {
        return peerRequest(peer, service, null, PEER_TIMEOUT_MS);
    }

    private static JsonNode peerRequest(String peer, String service, ObjectNode extra, int timeoutMs) {
        ZMQ.Socket req = null;
        try {
            req = context.createSocket(ZMQ.REQ);
            req.setLinger(0);
            req.setSendTimeOut(timeoutMs);
            req.setReceiveTimeOut(timeoutMs);
            req.connect("tcp://" + peer + ":" + PEER_PORT);

            ObjectNode msg = newRequest(service);
            if (extra != null) {
                msg.setAll(extra);
            }
            stamp(msg);
            logMessage("ENVIADO (-> " + peer + ")", msg);
            if (!req.send(msgpack.writeValueAsBytes(msg), 0)) {
                return null;
            }

            byte[] raw = req.recv(0);
            if (raw == null) {
                return null;
            }
            JsonNode resp = msgpack.readTree(raw);
            observe(resp);
            logMessage("RECEBIDO (<- " + peer + ")", resp);
            return resp;
        } catch (Exception e) {
            log.warn("[{}] Sem comunicacao com {}: {}", SERVER_NAME, peer, e.getMessage());
            return null;
        } finally {
            if (req != null) {
                context.destroySocket(req);
            }
        }
    }

    private static void startElection() throws IOException {
        JsonNode rep = referenceRequest(newRequest("list"));

        List<String> higher = new ArrayList<>();
        for (JsonNode s : rep.path("servers")) {
            String name = s.path("name").asText("");
            if (s.path("rank").asInt(0) > myRank && !name.equals(SERVER_NAME)) {
                higher.add(name);
            }
        }

        log.info("[{}] Iniciando eleicao. Servidores com rank maior: {}", SERVER_NAME, higher);

        boolean gotOk = false;
        for (String peer : higher) {
            JsonNode resp = peerRequest(peer, "eleicao");
            if (resp != null && "OK".equalsIgnoreCase(resp.path("status").asText(""))) {
                gotOk = true;
                break;
            }
        }

        if (!gotOk) {
            coordinatorName = SERVER_NAME;
            log.info("[{}] Eleito como coordenador", SERVER_NAME);
            announceCoordinator();
        }
    }

    private static void announceCoordinator() {
        try {
            ObjectNode payload = stamp(msgpack.createObjectNode());
            payload.put("coordinator", SERVER_NAME);
            logMessage("PUBLICANDO no topico 'servers'", payload);
            pubSocket.sendMore("servers".getBytes(StandardCharsets.UTF_8));
            pubSocket.send(msgpack.writeValueAsBytes(payload), 0);
        } catch (IOException e) {
            log.warn("[{}] Erro ao anunciar coordenador: {}", SERVER_NAME, e.getMessage());
        }
    }

    private static void syncClock() throws IOException {
        if (SERVER_NAME.equals(coordinatorName)) {
            return; // o coordenador e a referencia de tempo
        }
        if (coordinatorName == null) {
            startElection();
            return;
        }

        double t0 = System.currentTimeMillis() / 1000.0;
        JsonNode resp = peerRequest(coordinatorName, "relogio");
        double t1 = System.currentTimeMillis() / 1000.0;

        if (resp == null) {
            log.warn("[{}] Coordenador {} indisponivel, iniciando eleicao", SERVER_NAME, coordinatorName);
            coordinatorName = null;
            startElection();
            return;
        }

        if ("OK".equalsIgnoreCase(resp.path("status").asText(""))) {
            double coordinatorTime = resp.path("time").asDouble();
            clockOffset = coordinatorTime + (t1 - t0) / 2.0 - t1;
            log.info("[{}] Relogio sincronizado com {}. Ajuste: {} s", SERVER_NAME, coordinatorName,
                    String.format("%.4f", clockOffset));
        }
    }

    private static void handlePeerRequest() {
        byte[] raw = peerSocket.recv(0);
        if (raw == null) {
            return;
        }

        String service = "";
        ObjectNode resp = msgpack.createObjectNode();
        try {
            JsonNode req = msgpack.readTree(raw);
            observe(req);
            logMessage("RECEBIDO (peer)", req);
            service = req.path("service").asText("");

            switch (service) {
                case "eleicao":
                    resp.put("status", "OK");
                    break;
                case "relogio":
                    resp.put("status", "OK");
                    resp.put("time", now());
                    break;
                case "sync":
                    resp.put("status", "OK");
                    resp.set("ops", opsAfter(req.path("vector")));
                    break;
                default:
                    resp.put("status", "erro");
            }
        } catch (Exception e) {
            resp.put("status", "erro");
        }

        try {
            stamp(resp);
            logMessage("ENVIADO (peer)", resp);
            peerSocket.send(msgpack.writeValueAsBytes(resp), 0);
        } catch (IOException e) {
            peerSocket.send(new byte[0], 0);
        }

        if ("eleicao".equals(service)) {
            try {
                startElection();
            } catch (Exception e) {
                log.warn("[{}] Erro na eleicao: {}", SERVER_NAME, e.getMessage());
            }
        }
    }

    private static void handleSubscription() {
        byte[] topicBytes = subSocket.recv(0);
        if (topicBytes == null) {
            return;
        }
        byte[] raw = subSocket.hasReceiveMore() ? subSocket.recv(0) : null;
        if (raw == null) {
            return;
        }

        String topic = new String(topicBytes, StandardCharsets.UTF_8);
        try {
            JsonNode payload = msgpack.readTree(raw);
            observe(payload);

            if ("servers".equals(topic)) {
                logMessage("RECEBIDO (topico 'servers')", payload);
                String coordinator = payload.path("coordinator").asText("");
                if (!coordinator.isEmpty()) {
                    coordinatorName = coordinator;
                    log.info("[{}] Novo coordenador eleito e reconhecido: {}", SERVER_NAME, coordinatorName);
                }
            } else if (REPL_TOPIC.equals(topic)) {
                receiveReplicatedOp(payload);
            }
        } catch (IOException e) {
            log.warn("[{}] Mensagem invalida no topico {}: {}", SERVER_NAME, topic, e.getMessage());
        }
    }
}