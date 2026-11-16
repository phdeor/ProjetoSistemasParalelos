package com.sistema.model;

public class ClientMessagePub {
    private String sender;
    private String channel;
    private String message;
    private long timestamp;

    public ClientMessagePub() {
    }

    public ClientMessagePub(String sender, String channel, String message) {
        this.sender = sender;
        this.channel = channel;
        this.message = message;
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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}
