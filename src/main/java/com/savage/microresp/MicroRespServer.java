package com.savage.microresp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * Simple Redis-like server implementation using pure Java
 * Supports basic Redis protocol commands
 * In-memory only (stateless), but with Thread Safety.
 */
public class MicroRespServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroRespServer.class);

    private final int port;
    private final String password;
    private final Path dataDirectory;
    private final int maxConnections;
    
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledTaskService;
    private volatile boolean running = false;

    // Data stores (In-Memory Only)
    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private final Map<String, Long> expirationTimes = new ConcurrentHashMap<>();

    // Pub/Sub support
    // Use CopyOnWriteArrayList for thread-safe iteration during PUBLISH
    private final Map<String, List<ClientConnection>> channelSubscriptions = new ConcurrentHashMap<>();
    private final Map<String, ClientConnection> clientConnections = new ConcurrentHashMap<>();

    // Authentication support
    private final Set<String> authenticatedClients = ConcurrentHashMap.newKeySet();
    private int clientCounter = 0;

    // Inner class to track client connections
    private static class ClientConnection {
        final String clientId;
        final BufferedWriter writer;
        final Set<String> subscriptions = new HashSet<>();

        ClientConnection(String clientId, BufferedWriter writer) {
            this.clientId = clientId;
            this.writer = writer;
        }
    }

    public MicroRespServer(int port, String password, Path dataDirectory, int maxConnections) {
        this.port = port;
        this.password = password;
        this.dataDirectory = dataDirectory;
        this.maxConnections = maxConnections;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        // Fix: Use Java 21 Virtual Threads for high performance and scalability
        executorService = Executors.newVirtualThreadPerTaskExecutor();
        
        // Background tasks (Cleanup only)
        scheduledTaskService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });
        
        running = true;

        // Schedule expiration cleanup (every 10 seconds)
        scheduledTaskService.scheduleAtFixedRate(this::cleanupExpiredKeys, 
            10, 10, TimeUnit.SECONDS);

        LOGGER.info("MicroRESP server started on port {} (Max connections: {})", port, maxConnections);

        // Start client handler thread
        Thread acceptorThread = new Thread(this::acceptConnections, "MicroResp-Acceptor");
        acceptorThread.setDaemon(true);
        acceptorThread.start();
    }

    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, Long>> it = expirationTimes.entrySet().iterator();
        int removedCount = 0;
        
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (now > entry.getValue()) {
                dataStore.remove(entry.getKey());
                it.remove();
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            LOGGER.debug("Cleaned up {} expired keys", removedCount);
        }
    }

    private void acceptConnections() {
        try {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setKeepAlive(true); // Detect broken connections
                    String clientId = "client-" + (++clientCounter);
                    
                    // Manually enforce max connections since Virtual Thread executor is unbounded
                    if (clientConnections.size() >= maxConnections) {
                        LOGGER.warn("Max connections reached ({}), rejecting client {}", maxConnections, clientId);
                        clientSocket.close();
                        continue;
                    }

                    LOGGER.info("Accepted connection from {} (ID: {})", clientSocket.getRemoteSocketAddress(), clientId);
                    
                    try {
                        executorService.submit(() -> handleClient(clientSocket, clientId));
                    } catch (RejectedExecutionException e) {
                         // Should rare with virtual threads, but good practice
                        LOGGER.warn("Executor rejected client {}", clientId);
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    if (running) {
                        LOGGER.error("Error accepting client connection", e);
                    }
                }
            }
        } catch (Exception e) {
            if (running) {
                LOGGER.error("MicroRESP server error", e);
            }
        }
    }

    private void handleClient(Socket socket, String clientId) {
        ClientConnection clientConn;
        try {
            clientConn = new ClientConnection(clientId,
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
            clientConnections.put(clientId, clientConn);
            LOGGER.info("Client connected: {}. Active connections: {}", clientId, clientConnections.size());
        } catch (IOException e) {
            LOGGER.error("Failed to create client connection for {}", clientId, e);
            return;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
              BufferedWriter writer = clientConn.writer) {

            while (running) {
                String line = reader.readLine();
                if (line == null) break;

                // Parse Redis protocol
                if (line.startsWith("*")) { // Array
                    int commandCount = Integer.parseInt(line.substring(1));
                    String[] commands = new String[commandCount];

                    for (int i = 0; i < commandCount; i++) {
                        // Read bulk string
                        String bulkLine = reader.readLine();
                        if (bulkLine == null || !bulkLine.startsWith("$")) break;

                        int length = Integer.parseInt(bulkLine.substring(1));
                        if (length == -1) { // Null bulk string
                            commands[i] = null;
                        } else {
                            commands[i] = reader.readLine();
                        }
                    }

                    if (commands.length > 0 && commands[0] != null) {
                        handleCommand(commands, writer, clientId);
                    }
                }
            }
        } catch (Exception e) {
            // Normal disconnection
        } finally {
            // Clean up client connection
            clientConnections.remove(clientId);
            authenticatedClients.remove(clientId);
            
            // Remove from all channel subscriptions
            for (List<ClientConnection> subscribers : channelSubscriptions.values()) {
                subscribers.remove(clientConn);
            }

            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("Error closing socket", e);
            }
            LOGGER.info("Client disconnected: {}. Active connections: {}", clientId, clientConnections.size());
        }
    }

    private void handleCommand(String[] commands, BufferedWriter writer, String clientId) throws IOException {
        String command = commands[0].toUpperCase();

        // Check authentication for commands that require it
        boolean requiresAuth = !command.equals("AUTH") && !command.equals("PING") && !command.equals("QUIT");
        if (requiresAuth && password != null && !password.trim().isEmpty() && !authenticatedClients.contains(clientId)) {
            writer.write("-NOAUTH Authentication required.\r\n");
            writer.flush();
            return;
        }

        try {
            switch (command) {
                case "AUTH":
                    if (commands.length >= 2) {
                        String providedPassword = commands[1];
                        if (password == null || password.trim().isEmpty() || password.equals(providedPassword)) {
                            authenticatedClients.add(clientId);
                            writer.write("+OK\r\n");
                            writer.flush();
                            LOGGER.info("Client {} authenticated successfully", clientId);
                        } else {
                            writer.write("-ERR invalid password\r\n");
                            writer.flush();
                            LOGGER.warn("Client {} provided invalid password", clientId);
                        }
                    } else {
                        writer.write("-ERR wrong number of arguments for 'auth' command\r\n");
                        writer.flush();
                    }
                    break;

                case "PING":
                    writer.write("+PONG\r\n");
                    writer.flush();
                    break;
                
                case "SAVE":
                case "BGSAVE":
                    writer.write("-ERR persistence not supported in this mode\r\n");
                    writer.flush();
                    break;

                case "SET":
                    if (commands.length >= 3) {
                        String key = commands[1];
                        String value = commands[2];
                        dataStore.put(key, value);

                        // Handle expiration if provided
                        if (commands.length >= 5 && "PX".equalsIgnoreCase(commands[3])) {
                            long ttl = Long.parseLong(commands[4]);
                            expirationTimes.put(key, System.currentTimeMillis() + ttl);
                        } else {
                             // Remove any existing expiration if simple SET is used
                             expirationTimes.remove(key);
                        }

                        writer.write("+OK\r\n");
                        writer.flush();
                    } else {
                        writer.write("-ERR wrong number of arguments for 'set' command\r\n");
                        writer.flush();
                    }
                    break;

                case "GET":
                    if (commands.length >= 2) {
                        String key = commands[1];
                        
                        // Check expiration first
                        if (expirationTimes.containsKey(key)) {
                            if (System.currentTimeMillis() > expirationTimes.get(key)) {
                                dataStore.remove(key);
                                expirationTimes.remove(key);
                            }
                        }
                        
                        String value = dataStore.get(key);

                        if (value != null) {
                            writer.write("$" + value.length() + "\r\n");
                            writer.write(value + "\r\n");
                            writer.flush();
                        } else {
                            writer.write("$-1\r\n"); // Null bulk string
                            writer.flush();
                        }
                    } else {
                        writer.write("-ERR wrong number of arguments for 'get' command\r\n");
                        writer.flush();
                    }
                    break;

                case "DEL":
                    if (commands.length >= 2) {
                        String key = commands[1];
                        String value = dataStore.remove(key);
                        expirationTimes.remove(key);

                        if (value != null) {
                            writer.write(":1\r\n"); // Integer reply
                        } else {
                            writer.write(":0\r\n");
                        }
                        writer.flush();
                    } else {
                        writer.write("-ERR wrong number of arguments for 'del' command\r\n");
                        writer.flush();
                    }
                    break;

                case "EXISTS":
                    if (commands.length >= 2) {
                        String key = commands[1];
                        boolean exists = dataStore.containsKey(key);

                        // Check expiration
                        if (exists && expirationTimes.containsKey(key)) {
                            if (System.currentTimeMillis() > expirationTimes.get(key)) {
                                dataStore.remove(key);
                                expirationTimes.remove(key);
                                exists = false;
                            }
                        }

                        writer.write(":" + (exists ? "1" : "0") + "\r\n");
                        writer.flush();
                    } else {
                        writer.write("-ERR wrong number of arguments for 'exists' command\r\n");
                        writer.flush();
                    }
                    break;

                case "KEYS":
                     // Basic implementation for KEYS * (use with caution)
                     if (commands.length >= 2) {
                        String pattern = commands[1];
                        // Only support * for now
                        if ("*".equals(pattern)) {
                            Set<String> keys = new HashSet<>(dataStore.keySet());
                            // Filter expired
                            long now = System.currentTimeMillis();
                            keys.removeIf(k -> expirationTimes.containsKey(k) && now > expirationTimes.get(k));
                            
                            writer.write("*" + keys.size() + "\r\n");
                            for (String k : keys) {
                                writer.write("$" + k.length() + "\r\n");
                                writer.write(k + "\r\n");
                            }
                            writer.flush();
                        } else {
                             writer.write("*0\r\n"); // No pattern support yet
                             writer.flush();
                        }
                     } else {
                        writer.write("-ERR wrong number of arguments for 'keys' command\r\n");
                        writer.flush();
                     }
                    break;

                case "SUBSCRIBE":
                    if (commands.length >= 2) {
                        String channel = commands[1];
                        ClientConnection clientConn = clientConnections.get(clientId);

                        // Add to channel subscriptions
                        channelSubscriptions.computeIfAbsent(channel, k -> new CopyOnWriteArrayList<>()).add(clientConn);
                        clientConn.subscriptions.add(channel);

                        // Send subscription confirmation
                        writer.write("*3\r\n");
                        writer.write("$9\r\n");
                        writer.write("subscribe\r\n");
                        writer.write("$" + channel.length() + "\r\n");
                        writer.write(channel + "\r\n");
                        writer.write(":1\r\n"); // Number of subscriptions
                        writer.flush();
                        LOGGER.info("Client {} subscribed to channel: {}", clientId, channel);
                    } else {
                        writer.write("-ERR wrong number of arguments for 'subscribe' command\r\n");
                        writer.flush();
                    }
                    break;

                case "PUBLISH":
                    if (commands.length >= 3) {
                        String channel = commands[1];
                        String message = commands[2];

                        // Get subscribers for this channel
                        List<ClientConnection> subscribers = channelSubscriptions.get(channel);
                        int recipientCount = 0;

                        if (subscribers != null) {
                            // Send message to all subscribers
                            // CopyOnWriteArrayList allows safe iteration without copying
                            for (ClientConnection subscriber : subscribers) {
                                try {
                                    subscriber.writer.write("*3\r\n");
                                    subscriber.writer.write("$7\r\n");
                                    subscriber.writer.write("message\r\n");
                                    subscriber.writer.write("$" + channel.length() + "\r\n");
                                    subscriber.writer.write(channel + "\r\n");
                                    subscriber.writer.write("$" + message.length() + "\r\n");
                                    subscriber.writer.write(message + "\r\n");
                                    subscriber.writer.flush();
                                    recipientCount++;
                                } catch (IOException e) {
                                    // Remove failed subscriber
                                    subscribers.remove(subscriber);
                                }
                            }
                        }

                        // Respond with number of recipients
                        writer.write(":" + recipientCount + "\r\n");
                        writer.flush();
                    } else {
                        writer.write("-ERR wrong number of arguments for 'publish' command\r\n");
                        writer.flush();
                    }
                    break;

                case "UNSUBSCRIBE":
                    ClientConnection clientConn = clientConnections.get(clientId);
                    if (commands.length >= 2) {
                        String channel = commands[1];
                        List<ClientConnection> subscribers = channelSubscriptions.get(channel);
                        if (subscribers != null) {
                            subscribers.remove(clientConn);
                            if (subscribers.isEmpty()) {
                                channelSubscriptions.remove(channel);
                            }
                        }
                        clientConn.subscriptions.remove(channel);

                        writer.write("*3\r\n");
                        writer.write("$11\r\n");
                        writer.write("unsubscribe\r\n");
                        writer.write("$" + channel.length() + "\r\n");
                        writer.write(channel + "\r\n");
                        writer.write(":" + clientConn.subscriptions.size() + "\r\n"); 
                        writer.flush();
                    } else {
                        for (String channel : new HashSet<>(clientConn.subscriptions)) {
                            List<ClientConnection> subscribers = channelSubscriptions.get(channel);
                            if (subscribers != null) {
                                subscribers.remove(clientConn);
                                if (subscribers.isEmpty()) {
                                    channelSubscriptions.remove(channel);
                                }
                            }
                        }
                        int remainingSubs = clientConn.subscriptions.size();
                        clientConn.subscriptions.clear();

                        writer.write("*3\r\n");
                        writer.write("$11\r\n");
                        writer.write("unsubscribe\r\n");
                        writer.write("$0\r\n");
                        writer.write("\r\n");
                        writer.write(":" + remainingSubs + "\r\n");
                        writer.flush();
                    }
                    break;

                default:
                    writer.write("-ERR unknown command '" + command + "'\r\n");
                    writer.flush();
                    break;
            }
        } catch (Exception e) {
            writer.write("-ERR " + e.getMessage() + "\r\n");
            writer.flush();
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.error("Error closing server socket", e);
        }

        if (scheduledTaskService != null) {
            scheduledTaskService.shutdownNow();
        }

        if (executorService != null) {
            executorService.shutdownNow();
        }

        LOGGER.info("MicroRESP server stopped");
    }

    public boolean isRunning() {
        return running;
    }

    public int getPort() {
        return port;
    }
}
