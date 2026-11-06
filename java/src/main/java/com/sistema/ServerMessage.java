package com.sistema;

public class ServerMessage {
    private String sender;
    private ServerStatusEnum status;
    private String message;

    public ServerMessage() {
    }

    public ServerMessage(String sender, ServerStatusEnum status, String message) {
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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
