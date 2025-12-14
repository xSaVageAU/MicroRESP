package com.savage.microresp.internal.server;

import com.savage.microresp.internal.core.PubSubManager;
import com.savage.microresp.internal.core.RespStore;
import com.savage.microresp.internal.protocol.RespProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

public class MicroRespServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroRespServer.class);

    private final int port;
    private final String password;
    private final String bindAddress;
    private final int timeoutMillis;
    private final int maxConnections;
    
    // Core Components
    private final RespStore respStore;
    private final PubSubManager pubSubManager;
    private final Map<String, ClientSession> activeSessions = new ConcurrentHashMap<>();

    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledTaskService;
    private volatile boolean running = false;
    private int clientCounter = 0;

    public MicroRespServer(int port, String password, int maxConnections, String bindAddress, int timeoutMillis) {
        this.port = port;
        this.password = password;
        this.maxConnections = maxConnections;
        this.bindAddress = bindAddress;
        this.timeoutMillis = timeoutMillis;
        
        this.respStore = new RespStore();
        this.pubSubManager = new PubSubManager();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port, 50, java.net.InetAddress.getByName(bindAddress));
        
        // Use Java 21 Virtual Threads
        executorService = Executors.newVirtualThreadPerTaskExecutor();
        
        // Background tasks
        scheduledTaskService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });
        
        running = true;

        // Schedule expiration cleanup
        scheduledTaskService.scheduleAtFixedRate(respStore::cleanupExpired, 10, 10, TimeUnit.SECONDS);

        LOGGER.info("MicroRESP server started on {}:{} (Max connections: {})", bindAddress, port, maxConnections);

        // Start acceptor thread
        Thread acceptorThread = new Thread(this::acceptConnections, "MicroResp-Acceptor");
        acceptorThread.setDaemon(true);
        acceptorThread.start();
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error closing server socket", e);
        }
        if (scheduledTaskService != null) scheduledTaskService.shutdownNow();
        if (executorService != null) executorService.shutdownNow();
        
        LOGGER.info("MicroRESP server stopped");
    }

    public boolean isRunning() {
        return running;
    }

    private void acceptConnections() {
        try {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setKeepAlive(true);
                    if (timeoutMillis > 0) {
                        clientSocket.setSoTimeout(timeoutMillis);
                    }
                    
                    String clientId = "client-" + (++clientCounter);

                    if (activeSessions.size() >= maxConnections) {
                        LOGGER.warn("Max connections reached ({}), rejecting client {}", maxConnections, clientId);
                        clientSocket.close();
                        continue;
                    }

                    LOGGER.info("Accepted connection from {} (ID: {})", clientSocket.getRemoteSocketAddress(), clientId);
                    executorService.submit(() -> handleClient(clientSocket, clientId));

                } catch (IOException e) {
                    if (running) LOGGER.error("Error accepting client connection", e);
                }
            }
        } catch (Exception e) {
            if (running) LOGGER.error("MicroRESP server error", e);
        }
    }

    private void handleClient(Socket socket, String clientId) {
        ClientSession session;
        try {
            session = new ClientSession(clientId, new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
            activeSessions.put(clientId, session);
        } catch (IOException e) {
            LOGGER.error("Failed to create session for {}", clientId, e);
            return;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            while (running) {
                String line = reader.readLine();
                if (line == null) break;

                if (line.startsWith("*")) {
                    int count = Integer.parseInt(line.substring(1));
                    String[] commands = new String[count];
                    for (int i = 0; i < count; i++) {
                        String bulkLen = reader.readLine();
                        if (bulkLen == null || !bulkLen.startsWith("$")) break;
                        int len = Integer.parseInt(bulkLen.substring(1));
                        if (len == -1) commands[i] = null;
                        else commands[i] = reader.readLine();
                    }
                    
                    if (commands.length > 0 && commands[0] != null) {
                        handleCommand(session, commands);
                    }
                }
            }
        } catch (Exception e) {
            // Client disconnect
        } finally {
            cleanupSession(session, socket);
        }
    }

    private void cleanupSession(ClientSession session, Socket socket) {
        activeSessions.remove(session.getId());
        pubSubManager.removeClient(session);
        try {
            socket.close();
        } catch (IOException e) {
            LOGGER.error("Error closing socket for {}", session.getId(), e);
        }
        LOGGER.info("Client disconnected: {}", session.getId());
    }

    private void handleCommand(ClientSession session, String[] args) throws IOException {
        String cmd = args[0].toUpperCase();
        BufferedWriter writer = session.getWriter();

        // Auth Check
        boolean isAuthCmd = cmd.equals("AUTH") || cmd.equals("PING") || cmd.equals("QUIT");
        if (!isAuthCmd && password != null && !password.isEmpty() && !session.isAuthenticated()) {
            RespProtocol.writeError(writer, "NOAUTH Authentication required.");
            return;
        }

        try {
            switch (cmd) {
                case "PING":
                    RespProtocol.writeSimpleString(writer, "PONG");
                    break;
                case "AUTH":
                    if (args.length < 2) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'auth' command");
                    } else if (password == null || password.isEmpty() || password.equals(args[1])) {
                        session.setAuthenticated(true);
                        RespProtocol.writeSimpleString(writer, "OK");
                    } else {
                        RespProtocol.writeError(writer, "invalid password");
                    }
                    break;
                case "SET":
                    if (args.length < 3) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'set' command");
                    } else {
                        long ttl = 0;
                        if (args.length >= 5 && "PX".equalsIgnoreCase(args[3])) {
                            try { ttl = Long.parseLong(args[4]); } catch (NumberFormatException ignored) {}
                        }
                        respStore.set(args[1], args[2], ttl);
                        RespProtocol.writeSimpleString(writer, "OK");
                    }
                    break;
                case "GET":
                    if (args.length < 2) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'get' command");
                    } else {
                        String val = respStore.get(args[1]);
                        RespProtocol.writeBulkString(writer, val);
                    }
                    break;
                case "DEL":
                    if (args.length < 2) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'del' command");
                    } else {
                        int count = respStore.del(args[1]);
                        RespProtocol.writeInteger(writer, count);
                    }
                    break;
                case "EXISTS":
                    if (args.length < 2) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'exists' command");
                    } else {
                        boolean exists = respStore.exists(args[1]);
                        RespProtocol.writeInteger(writer, exists ? 1 : 0);
                    }
                    break;
                case "PUBLISH":
                    if (args.length < 3) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'publish' command");
                    } else {
                        int count = pubSubManager.publish(args[1], args[2]);
                        RespProtocol.writeInteger(writer, count);
                    }
                    break;
                case "SUBSCRIBE":
                    if (args.length < 2) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'subscribe' command");
                    } else {
                        // Support multiple channels
                        for (int i = 1; i < args.length; i++) {
                            String channel = args[i];
                            int subs = pubSubManager.subscribe(session, channel);
                            // Write standard subscribe response for EACH channel
                            RespProtocol.writeArrayHeader(writer, 3);
                            RespProtocol.writeBulkString(writer, "subscribe");
                            RespProtocol.writeBulkString(writer, channel);
                            RespProtocol.writeInteger(writer, subs);
                        }
                    }
                    break;
                case "UNSUBSCRIBE":
                    if (args.length < 2) {
                        // Unsubscribe all ? (Lazy implementation: just clear all manually for now)
                        // Ideally we loop subs. Current impl requires arg.
                         RespProtocol.writeError(writer, "wrong number of arguments for 'unsubscribe' command");
                    } else {
                        for (int i = 1; i < args.length; i++) {
                            String channel = args[i];
                            int subs = pubSubManager.unsubscribe(session, channel);
                            RespProtocol.writeArrayHeader(writer, 3);
                            RespProtocol.writeBulkString(writer, "unsubscribe");
                            RespProtocol.writeBulkString(writer, channel);
                            RespProtocol.writeInteger(writer, subs);
                        }
                    }
                    break;
                case "KEYS":
                     if (args.length < 2) {
                        RespProtocol.writeError(writer, "wrong number of arguments for 'keys' command");
                     } else if ("*".equals(args[1])) {
                         var keys = respStore.keys("*");
                         RespProtocol.writeArrayHeader(writer, keys.size());
                         for (String key : keys) {
                             RespProtocol.writeBulkString(writer, key);
                         }
                     } else {
                         RespProtocol.writeArrayHeader(writer, 0);
                     }
                     break;
                case "SAVE":
                case "BGSAVE":
                    RespProtocol.writeError(writer, "persistence not supported");
                    break;
                default:
                    RespProtocol.writeError(writer, "unknown command '" + cmd + "'");
            }
        } catch (Exception e) {
            RespProtocol.writeError(writer, "Internal Error: " + e.getMessage());
        }
    }
}
