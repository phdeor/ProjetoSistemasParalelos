package com.sistema.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ClientMessage {
    private String sender;
    private String channel;
    private JsonNode content;
    private long timestamp;

    public ClientMessage() {
    }

    public ClientMessage(String sender, String channel, JsonNode content) {
        this.sender = sender;
        this.channel = channel;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public JsonNode getContent() {
        return content;
    }

    public void setContent(JsonNode content) {
        this.content = content;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public <T> T getContentAs(Class<T> type, ObjectMapper mapper) {
        try {
            return mapper.treeToValue(content, type);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

}