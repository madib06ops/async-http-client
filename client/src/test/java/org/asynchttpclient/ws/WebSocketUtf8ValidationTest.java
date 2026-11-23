/*
 *    Copyright (c) 2026 AsyncHttpClient Project. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.asynchttpclient.ws;

import org.asynchttpclient.AsyncHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A raw server completes the handshake and then sends text frames whose payload is not UTF-8. RFC 6455 section
 * 8.1 requires the client to fail the connection, section 7.4.1 with status 1007, rather than hand the listener a
 * decoded string with the invalid bytes replaced. Jetty refuses to send such a frame, hence the raw socket.
 */
public class WebSocketUtf8ValidationTest {

    private ServerSocket serverSocket;
    private Thread serverThread;
    // The close status the server reads from the client, or -1 when the client closes the socket without one.
    private CompletableFuture<Integer> clientCloseCode;

    @AfterEach
    public void tearDown() throws Exception {
        if (serverSocket != null) {
            serverSocket.close();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
    }

    private int startServer(byte[]... frames) throws IOException {
        serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"));
        clientCloseCode = new CompletableFuture<>();
        int port = serverSocket.getLocalPort();
        serverThread = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                InputStream in = socket.getInputStream();
                String key = readHandshakeKey(in);
                OutputStream out = socket.getOutputStream();
                out.write(("HTTP/1.1 101 Switching Protocols\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Sec-WebSocket-Accept: " + WebSocketUtils.getAcceptKey(key) + "\r\n"
                        + "\r\n").getBytes(StandardCharsets.US_ASCII));
                out.flush();
                // Sent only once the client reports the upgrade complete, so the frames are read as frames,
                // not as the tail of the handshake response.
                while (readFrame(in).opcode != 0x1) {
                    // not the client's text frame yet
                }
                for (byte[] frame : frames) {
                    out.write(frame);
                }
                out.flush();
                clientCloseCode.complete(readCloseCode(in));
            } catch (Exception e) {
                clientCloseCode.completeExceptionally(e);
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        return port;
    }

    private static String readHandshakeKey(InputStream in) throws IOException {
        ByteArrayOutputStream request = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            request.write(b);
            byte[] bytes = request.toByteArray();
            int n = bytes.length;
            if (n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n' && bytes[n - 2] == '\r' && bytes[n - 1] == '\n') {
                break;
            }
        }
        for (String line : request.toString(StandardCharsets.US_ASCII.name()).split("\r\n")) {
            if (line.regionMatches(true, 0, "Sec-WebSocket-Key:", 0, "Sec-WebSocket-Key:".length())) {
                return line.substring("Sec-WebSocket-Key:".length()).trim();
            }
        }
        throw new IOException("No Sec-WebSocket-Key in the handshake request");
    }

    private static final class ClientFrame {
        final int opcode;
        final byte[] payload;

        ClientFrame(int opcode, byte[] payload) {
            this.opcode = opcode;
            this.payload = payload;
        }
    }

    // Client frames are masked, and the ones this test provokes are short, so a 7-bit length is all the header carries.
    private static ClientFrame readFrame(InputStream in) throws IOException {
        int b0 = in.read();
        int b1 = in.read();
        if (b0 == -1 || b1 == -1) {
            return new ClientFrame(-1, new byte[0]);
        }
        int length = b1 & 0x7F;
        if (length >= 126) {
            throw new IOException("Unexpected client frame length " + length);
        }
        byte[] mask = new byte[4];
        readFully(in, mask);
        byte[] payload = new byte[length];
        readFully(in, payload);
        for (int i = 0; i < length; i++) {
            payload[i] ^= mask[i % 4];
        }
        return new ClientFrame(b0 & 0x0F, payload);
    }

    // The status of the client's close frame, 1005 for one without a status, -1 when the socket closes first.
    private static int readCloseCode(InputStream in) throws IOException {
        while (true) {
            ClientFrame frame = readFrame(in);
            if (frame.opcode == -1) {
                return -1;
            }
            if (frame.opcode == 0x8) {
                return frame.payload.length >= 2 ? (frame.payload[0] & 0xFF) << 8 | frame.payload[1] & 0xFF : 1005;
            }
        }
    }

    private static void readFully(InputStream in, byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = in.read(buffer, offset, buffer.length - offset);
            if (read == -1) {
                throw new IOException("Connection closed inside a frame");
            }
            offset += read;
        }
    }

    private static byte[] textFrame(boolean finalFragment, boolean continuation, int... payload) {
        byte[] frame = new byte[2 + payload.length];
        frame[0] = (byte) ((finalFragment ? 0x80 : 0) | (continuation ? 0x0 : 0x1));
        frame[1] = (byte) payload.length;
        for (int i = 0; i < payload.length; i++) {
            frame[2 + i] = (byte) payload[i];
        }
        return frame;
    }

    private List<String> connect(int port) throws Exception {
        List<String> texts = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        try (AsyncHttpClient client = asyncHttpClient()) {
            WebSocket webSocket = client.prepareGet("ws://localhost:" + port + "/")
                    .execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketListener() {
                        @Override
                        public void onOpen(WebSocket websocket) {
                        }

                        @Override
                        public void onClose(WebSocket websocket, int code, String reason) {
                            closed.complete(null);
                        }

                        @Override
                        public void onError(Throwable t) {
                            closed.complete(null);
                        }

                        @Override
                        public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                            texts.add(payload);
                        }
                    }).build()).get();
            webSocket.sendTextFrame("ready");
            // Wait on the server's view, which does not depend on whether the client refused the frame.
            int code = clientCloseCode.get(10, TimeUnit.SECONDS);
            if (code != -1) {
                closed.get(10, TimeUnit.SECONDS);
            }
            texts.add(0, Integer.toString(code));
        }
        return texts;
    }

    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void invalidUtf8TextFrameFailsTheConnectionWith1007() throws Exception {
        // 0xC3 starts a 2-byte sequence, 0x28 is not a continuation byte.
        int port = startServer(textFrame(true, false, 0xC3, 0x28));
        List<String> result = connect(port);
        assertEquals("1007", result.get(0), "RFC 6455 section 7.4.1: inconsistent data closes with 1007");
        assertEquals(1, result.size(), "the invalid text must not reach the listener: " + result);
    }

    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void fragmentedTextMessageIsValidatedAsAWhole() throws Exception {
        // Each fragment is a truncated sequence; together they are still invalid.
        int port = startServer(textFrame(false, false, 0xC3), textFrame(true, true, 0x28));
        List<String> result = connect(port);
        assertEquals("1007", result.get(0));
        assertEquals(1, result.size(), "the invalid text must not reach the listener: " + result);
    }

    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void validUtf8TextFrameIsDelivered() throws Exception {
        // "h\u00e9llo" in UTF-8, then a close so the client ends the exchange.
        int port = startServer(textFrame(true, false, 0x68, 0xC3, 0xA9, 0x6C, 0x6C, 0x6F),
                new byte[]{(byte) 0x88, 0x02, 0x03, (byte) 0xE8});
        List<String> result = connect(port);
        assertEquals(2, result.size(), result.toString());
        assertEquals("h\u00e9llo", result.get(1));
        assertTrue(!"1007".equals(result.get(0)), "valid UTF-8 must not be refused: " + result);
    }
}
