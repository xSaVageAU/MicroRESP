package com.savage.microresp.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

/**
 * STRICTLY FOR INTERNAL STARTUP HEALTH CHECKS.
 * Do not expose this as a public API.
 * 
 * Simple Redis client implementation using pure Java.
 * Supports basic Redis protocol commands.
 */
public class InternalHealthClient implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalHealthClient.class);

    private final String host;
    private final int port;
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public InternalHealthClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        LOGGER.debug("Connected to MicroRESP server at {}:{}", host, port);
    }

    public void auth(String password) throws IOException {
        sendCommand("AUTH", password);
        String response = readSimpleString();
        if (!"OK".equals(response)) {
            throw new IOException("Authentication failed: " + response);
        }
        LOGGER.debug("Authenticated with MicroRESP server");
    }

    public String ping() throws IOException {
        sendCommand("PING");
        return readSimpleString();
    }

    private void sendCommand(String... parts) throws IOException {
        // Send array
        writer.write("*" + parts.length + "\r\n");

        // Send each part as bulk string
        for (String part : parts) {
            if (part == null) {
                writer.write("$" + (-1) + "\r\n");
            } else {
                byte[] bytes = part.getBytes();
                writer.write("$" + bytes.length + "\r\n");
                writer.write(part + "\r\n");
            }
        }

        writer.flush();
    }

    private String readSimpleString() throws IOException {
        String line = reader.readLine();
        if (line == null) throw new IOException("Connection closed");
        if (!line.startsWith("+")) throw new IOException("Expected simple string, got: " + line);
        return line.substring(1);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
            LOGGER.debug("Disconnected from MicroRESP server");
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }
}
