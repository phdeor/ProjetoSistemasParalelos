package com.sistema;

public class ClientMessage {
    private String sender;
    private String channel;
    private String content;
    private long timestamp;

    // Construtor vazio (obrigatório para serialização)
    public ClientMessage() {
    }

    public ClientMessage(String sender, String channel, String content) {
        this.sender = sender;
        this.channel = channel;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    // Getters e Setters
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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}