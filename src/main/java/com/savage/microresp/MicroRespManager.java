package com.savage.microresp;

import com.savage.microresp.internal.InternalHealthClient;
import com.savage.microresp.internal.server.MicroRespServer;
import net.fabricmc.loader.api.FabricLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MicroRespManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroRespManager.class);

    private final MicroRespConfig config;
    private final Path configDirectory;

    private MicroRespServer microRespServer;

    public MicroRespManager(MicroRespConfig config) {
        this.config = config;
        this.configDirectory = FabricLoader.getInstance().getConfigDir().resolve("microresp");
    }

    public void initialize() {
        try {
            // Create config directory if it doesn't exist
            Files.createDirectories(configDirectory);

            LOGGER.info("MicroRESP Manager initialized");
        } catch (IOException e) {
            LOGGER.error("Failed to create directories", e);
        }
    }

    public void start() {
        // Check if Redis is actually running by trying to connect
        boolean actuallyRunning = false;
        try (InternalHealthClient testClient = new InternalHealthClient("localhost", config.getPort())) {
            testClient.connect();
            testClient.ping();
            actuallyRunning = true;
        } catch (Exception e) {
            // Redis is not actually running
            actuallyRunning = false;
        }

        if (actuallyRunning) {
            LOGGER.info("MicroRESP server is already running on port {}", config.getPort());
            return;
        }

        try {
            LOGGER.info("Starting MicroRESP server on port {}", config.getPort());

            // Start simple Redis server
            microRespServer = new MicroRespServer(
                config.getPort(), 
                config.getPassword(), 
                config.getMaxConnections(),
                config.getBindAddress(),
                config.getTimeoutMillis()
            );
            microRespServer.start();

            // Self-Test connection
            try (InternalHealthClient healthClient = new InternalHealthClient("localhost", config.getPort())) {
                healthClient.connect();
                // Authenticate if password is set
                if (config.hasPassword()) {
                    healthClient.auth(config.getPassword());
                }
                String pingResponse = healthClient.ping();
                LOGGER.info("MicroRESP internal health check successful: {}", pingResponse);
            }

            LOGGER.info("MicroRESP started successfully on port {}", config.getPort());
        } catch (IOException e) {
            LOGGER.error("Failed to start MicroRESP server", e);
            if (microRespServer != null) {
                microRespServer.stop();
            }
        }
    }

    public void stop() {
        if (microRespServer != null) {
            microRespServer.stop();
            microRespServer = null;
        }
        LOGGER.info("MicroRESP stopped successfully");
    }
}
