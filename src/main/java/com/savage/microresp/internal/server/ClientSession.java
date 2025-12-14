package com.savage.microresp.internal.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ClientSession {
    private final String clientId;
    private final BufferedWriter writer;
    private final java.net.Socket socket;
    private final Set<String> subscriptions = new HashSet<>();
    private boolean authenticated = false;

    public ClientSession(String clientId, java.net.Socket socket, BufferedWriter writer) {
        this.clientId = clientId;
        this.socket = socket;
        this.writer = writer;
    }

    public String getId() {
        return clientId;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public java.net.Socket getSocket() {
        return socket;
    }

    public Set<String> getSubscriptions() {
        return subscriptions;
    }

    public void addSubscription(String channel) {
        subscriptions.add(channel);
    }

    public void removeSubscription(String channel) {
        subscriptions.remove(channel);
    }

    public void clearSubscriptions() {
        subscriptions.clear();
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }
}
