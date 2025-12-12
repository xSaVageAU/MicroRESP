package com.savage.microresp;

import net.fabricmc.api.ModInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicroRespMod implements ModInitializer {
    public static final String MOD_ID = "microresp";
    public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

    private static MicroRespManager manager;

    @Override
    public void onInitialize() {
        LOGGER.info("MicroRESP initializing...");

        MicroRespConfig config = MicroRespConfig.load();
        manager = new MicroRespManager(config);
        
        manager.initialize();
        manager.start();

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (manager != null) {
                manager.stop();
            }
        }));
    }
}
