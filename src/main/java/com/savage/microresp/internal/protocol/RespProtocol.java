package com.savage.microresp.internal.protocol;

import java.io.BufferedWriter;
import java.io.IOException;

public class RespProtocol {

    private static final String CRLF = "\r\n";

    public static void writeSimpleString(BufferedWriter writer, String message) throws IOException {
        writer.write("+" + message + CRLF);
        writer.flush();
    }

    public static void writeError(BufferedWriter writer, String message) throws IOException {
        writer.write("-ERR " + message + CRLF);
        writer.flush();
    }

    public static void writeInteger(BufferedWriter writer, int value) throws IOException {
        writer.write(":" + value + CRLF);
        writer.flush();
    }

    public static void writeBulkString(BufferedWriter writer, String message) throws IOException {
        if (message == null) {
            writer.write("$-1" + CRLF);
        } else {
            writer.write("$" + message.length() + CRLF);
            writer.write(message + CRLF);
        }
        writer.flush();
    }

    public static void writeArrayHeader(BufferedWriter writer, int size) throws IOException {
        writer.write("*" + size + CRLF);
    }
    
    // For manual array construction (like PubSub messages)
    public static void writeArrayItem(BufferedWriter writer, String message) throws IOException {
        if (message == null) {
            writer.write("$-1" + CRLF);
        } else {
             writer.write("$" + message.length() + CRLF);
             writer.write(message + CRLF);
        }
    }
}
