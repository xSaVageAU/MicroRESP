package com.savage.microresp.internal.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;

public class RespStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(RespStore.class);

    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private final Map<String, Long> expirationTimes = new ConcurrentHashMap<>();

    public void set(String key, String value, long expirationMillis) {
        dataStore.put(key, value);
        if (expirationMillis > 0) {
            expirationTimes.put(key, System.currentTimeMillis() + expirationMillis);
        } else {
            expirationTimes.remove(key);
        }
    }

    public String get(String key) {
        checkExpiration(key);
        return dataStore.get(key);
    }

    public boolean exists(String key) {
        checkExpiration(key);
        return dataStore.containsKey(key);
    }

    public int del(String key) {
        String val = dataStore.remove(key);
        expirationTimes.remove(key);
        return val != null ? 1 : 0;
    }

    public Set<String> keys(String pattern) {
        // Only supports "*" for now
        Set<String> keys = new HashSet<>(dataStore.keySet());
        long now = System.currentTimeMillis();
        keys.removeIf(k -> expirationTimes.containsKey(k) && now > expirationTimes.get(k));
        return keys;
    }

    private void checkExpiration(String key) {
        if (expirationTimes.containsKey(key)) {
            if (System.currentTimeMillis() > expirationTimes.get(key)) {
                dataStore.remove(key);
                expirationTimes.remove(key);
            }
        }
    }

    public void cleanupExpired() {
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
}
