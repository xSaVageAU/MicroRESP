# MicroRESP ⚡

**A lightweight, embedded Redis-compatible server for Minecraft Fabric.**

MicroRESP provides a simple, in-process RESP (Redis Serialization Protocol) server that runs alongside your Minecraft server. It allows other mods to communicate using standard Pub/Sub and Key-Value commands without requiring an external Redis database installation.

## Who is this for?
MicroRESP is designed for **Developers** and **Server Admins** who need:
*   **Inter-mod communication** via Pub/Sub (e.g., cross-server chat, economy syncing).
*   **Simple Key-Value caching** for transient data.
*   **Zero-setup required**: No Docker, no external hosting, no `apt-get install redis`. Just drop the mod in the `mods` folder.

## ✅ What it DOES
*   ✅ **High Performance**: Powered by **Java 21 Virtual Threads**, handling thousands of concurrent connections with minimal overhead.
*   ✅ **Embedded Server**: Starts automatically on port `6379` (configurable).
*   ✅ **Standard Protocol**: Compatible with standard Java Redis clients like **Lettuce** or **Jedis**.
*   ✅ **Pub/Sub**: Full support for `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`.
*   ✅ **Key-Value**: `SET`, `GET`, `DEL`, `EXISTS` (with optional expiration `PX`).
*   ✅ **Authentication**: Supports password protection.
*   ✅ **Clean Shutdown**: Uses Daemon threads to ensure your server shuts down instantly.

## ❌ What it does NOT do
*   ❌ **Persistence**: Data is **IN-MEMORY ONLY**. It is lost when the server stops. `SAVE` and `BGSAVE` commands are disabled.
*   ❌ **Complex Types**: No support for Lists, Sets, Hashes, or Sorted Sets.
*   ❌ **Clustering**: No support for Redis Cluster or Sentinel.

## Configuration
The configuration file is located at `config/microresp/microresp.json`.

```json
{
  "port": 6379,
  "password": "",
  "maxConnections": 100
}
```

##  Developer Usage

Since MicroRESP speaks standard Redis, you don't need to depend on this mod's code directly. just use your favorite Redis library!

**Example using Lettuce:**

```java
RedisClient client = RedisClient.create("redis://localhost:6379");
StatefulRedisConnection<String, String> connection = client.connect();
RedisCommands<String, String> syncCommands = connection.sync();

// Publish a message
syncCommands.publish("global_chat", "Hello from Fabric!");

// Subscribe
RedisPubSubCommands<String, String> pubSub = connection.async();
pubSub.subscribe("global_chat");
```
