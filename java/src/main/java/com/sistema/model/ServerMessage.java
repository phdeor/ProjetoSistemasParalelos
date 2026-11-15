package com.sistema.model;

import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ServerMessage {
    private String sender;
    private ServerStatusEnum status;
    private JsonNode message;
    private final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    public ServerMessage() {
    }

    public ServerMessage(String sender, ServerStatusEnum status, JsonNode message) {
        this.sender = sender;
        this.status = status;
        this.message = message;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public ServerStatusEnum getStatus() {
        return status;
    }

    public void setStatus(ServerStatusEnum status) {
        this.status = status;
    }

    public JsonNode getMessage() {
        return message;
    }

    public void setMessage(JsonNode message) {
        this.message = message;
    }

    @JsonIgnore
    public String getMessageString() {
        try {
            return mapper.treeToValue(message, String.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    @JsonIgnore
    public <T> T getMessageAs(Class<T> type) {
        try {
            return mapper.treeToValue(message, type);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
