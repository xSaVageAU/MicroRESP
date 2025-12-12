package com.savage.microresp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.fabricmc.loader.api.FabricLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MicroRespConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicroRespConfig.class);
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final String CONFIG_FILE = "microresp.json";

    private int port = 6379;
    private String bindAddress = "127.0.0.1";
    private int timeoutMillis = 0; // 0 = Infinite/No timeout
    private String password = "";
    private int maxConnections = 100;

    public static MicroRespConfig load() {
        Path configDir = FabricLoader.getInstance().getConfigDir().resolve("microresp");
        Path configPath = configDir.resolve(CONFIG_FILE);
        
        try {
            if (!Files.exists(configDir)) {
                Files.createDirectories(configDir);
            }
            
            if (Files.exists(configPath)) {
                return GSON.fromJson(Files.newBufferedReader(configPath), MicroRespConfig.class);
            } else {
                return save(new MicroRespConfig());
            }
        } catch (IOException e) {
            LOGGER.error("Failed to load config, using defaults", e);
        }
        return new MicroRespConfig();
    }

    public static MicroRespConfig save(MicroRespConfig config) {
        Path configDir = FabricLoader.getInstance().getConfigDir().resolve("microresp");
        Path configPath = configDir.resolve(CONFIG_FILE);
        
        try {
            if (!Files.exists(configDir)) {
                Files.createDirectories(configDir);
            }
            Files.writeString(configPath, GSON.toJson(config));
        } catch (IOException e) {
            LOGGER.error("Failed to save config", e);
        }
        return config;
    }

    public int getPort() {
        return port;
    }

    public String getPassword() {
        return password;
    }

    public boolean hasPassword() {
        return password != null && !password.trim().isEmpty();
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }
}
