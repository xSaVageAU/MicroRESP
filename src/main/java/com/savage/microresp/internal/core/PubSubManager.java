package com.savage.microresp.internal.core;

import com.savage.microresp.internal.protocol.RespProtocol;
import com.savage.microresp.internal.server.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PubSubManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubManager.class);

    // Use CopyOnWriteArrayList for thread-safe iteration during PUBLISH
    private final Map<String, List<ClientSession>> channelSubscriptions = new ConcurrentHashMap<>();

    public int subscribe(ClientSession client, String channel) {
        channelSubscriptions.computeIfAbsent(channel, k -> new CopyOnWriteArrayList<>()).add(client);
        client.addSubscription(channel);
        return client.getSubscriptions().size();
    }

    public int unsubscribe(ClientSession client, String channel) {
        List<ClientSession> subscribers = channelSubscriptions.get(channel);
        if (subscribers != null) {
            subscribers.remove(client);
            if (subscribers.isEmpty()) {
                channelSubscriptions.remove(channel);
            }
        }
        client.removeSubscription(channel);
        return client.getSubscriptions().size();
    }

    public void removeClient(ClientSession client) {
        // Efficiently remove from all subscribed channels
        for (String channel : client.getSubscriptions()) {
            List<ClientSession> subscribers = channelSubscriptions.get(channel);
            if (subscribers != null) {
                subscribers.remove(client);
                if (subscribers.isEmpty()) {
                    channelSubscriptions.remove(channel);
                }
            }
        }
        client.clearSubscriptions();
    }

    public int publish(String channel, String message) {
        List<ClientSession> subscribers = channelSubscriptions.get(channel);
        if (subscribers == null) return 0;

        int recipientCount = 0;
        for (ClientSession subscriber : subscribers) {
            try {
                // Manually write PubSub array response
                RespProtocol.writeArrayHeader(subscriber.getWriter(), 3);
                RespProtocol.writeArrayItem(subscriber.getWriter(), "message");
                RespProtocol.writeArrayItem(subscriber.getWriter(), channel);
                RespProtocol.writeArrayItem(subscriber.getWriter(), message);
                subscriber.getWriter().flush();
                recipientCount++;
            } catch (IOException e) {
                // Failed to write means client likely dead, will be cleaned up by main loop
            }
        }
        return recipientCount;
    }
}
