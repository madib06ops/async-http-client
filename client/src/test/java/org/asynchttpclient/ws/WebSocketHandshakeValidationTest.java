/*
 *    Copyright (c) 2024 AsyncHttpClient Project. All rights reserved.
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
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.ws.WebSocketUtils.getAcceptKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WebSocketHandshakeValidationTest {

    private ServerSocket serverSocket;
    private Thread serverThread;

    @AfterEach
    public void tearDown() throws Exception {
        if (serverSocket != null) {
            serverSocket.close();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
    }

    // Answers the one upgrade request with the response built from the accept key its Sec-WebSocket-Key
    // yields, then holds the socket open until the client closes it.
    private int startServer(Function<String, String> response) throws IOException {
        serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"));
        int port = serverSocket.getLocalPort();
        serverThread = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                InputStream in = socket.getInputStream();
                StringBuilder head = new StringBuilder();
                int b;
                while ((b = in.read()) != -1) {
                    head.append((char) b);
                    if (head.length() >= 4 && head.lastIndexOf("\r\n\r\n") == head.length() - 4) {
                        break;
                    }
                }
                OutputStream out = socket.getOutputStream();
                out.write(response.apply(getAcceptKey(headerValue(head.toString(), "Sec-WebSocket-Key")))
                        .getBytes(StandardCharsets.US_ASCII));
                out.flush();
                while (in.read() != -1) {
                    // the client decides when the connection ends
                }
            } catch (Exception ignored) {
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        return port;
    }

    private static String headerValue(String head, String name) {
        for (String line : head.split("\r\n")) {
            int colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).equalsIgnoreCase(name)) {
                return line.substring(colon + 1).trim();
            }
        }
        return null;
    }

    private static String handshake(String accept, String extraHeaders) {
        return "HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: " + accept + "\r\n"
                + extraHeaders
                + "\r\n";
    }

    private static WebSocketUpgradeHandler upgradeHandler(CountDownLatch openLatch) {
        return new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketListener() {
            @Override
            public void onOpen(WebSocket websocket) {
                openLatch.countDown();
            }

            @Override
            public void onClose(WebSocket websocket, int code, String reason) {
            }

            @Override
            public void onError(Throwable t) {
            }
        }).build();
    }

    private static void assertHandshakeRefused(int port, String offeredSubprotocols) throws Exception {
        CountDownLatch openLatch = new CountDownLatch(1);

        try (AsyncHttpClient c = asyncHttpClient()) {
            BoundRequestBuilder request = c.prepareGet("ws://localhost:" + port + "/");
            if (offeredSubprotocols != null) {
                request.setHeader(SEC_WEBSOCKET_PROTOCOL, offeredSubprotocols);
            }
            assertThrows(ExecutionException.class, () -> request.execute(upgradeHandler(openLatch)).get());

            assertEquals(1, openLatch.getCount(), "onOpen must not fire when the handshake response is invalid");
        }
    }

    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void doesNotUpgradeOnInvalidAcceptKey() throws Exception {
        int port = startServer(accept -> handshake("this-is-not-the-expected-accept", ""));
        CountDownLatch openLatch = new CountDownLatch(1);

        try (AsyncHttpClient c = asyncHttpClient()) {
            assertThrows(ExecutionException.class, () -> c.prepareGet("ws://localhost:" + port + "/")
                    .execute(upgradeHandler(openLatch)).get());

            assertFalse(openLatch.await(2, TimeUnit.SECONDS),
                    "onOpen must not fire when the server Sec-WebSocket-Accept is invalid");
        }
    }

    // RFC 6455 section 4.1 step 2: the Upgrade value has to be websocket, not merely present.
    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void doesNotUpgradeOnWrongUpgradeValue() throws Exception {
        int port = startServer(accept -> handshake(accept, "").replace("Upgrade: websocket", "Upgrade: h2c"));
        assertHandshakeRefused(port, null);
    }

    // RFC 6455 section 4.1 step 6: a subprotocol the client never offered.
    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void doesNotUpgradeOnSubprotocolWhenNoneOffered() throws Exception {
        int port = startServer(accept -> handshake(accept, "Sec-WebSocket-Protocol: chat\r\n"));
        assertHandshakeRefused(port, null);
    }

    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void doesNotUpgradeOnSubprotocolOtherThanOffered() throws Exception {
        int port = startServer(accept -> handshake(accept, "Sec-WebSocket-Protocol: other\r\n"));
        assertHandshakeRefused(port, "chat, superchat");
    }

    // RFC 6455 section 4.1 step 5: an extension the client never offered.
    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void doesNotUpgradeOnExtensionWhenNoneOffered() throws Exception {
        int port = startServer(accept -> handshake(accept, "Sec-WebSocket-Extensions: permessage-deflate\r\n"));
        assertHandshakeRefused(port, null);
    }

    @Test
    @Timeout(unit = TimeUnit.MILLISECONDS, value = 30000)
    public void upgradesOnOfferedSubprotocol() throws Exception {
        int port = startServer(accept -> handshake(accept, "Sec-WebSocket-Protocol: superchat\r\n"));
        CountDownLatch openLatch = new CountDownLatch(1);

        try (AsyncHttpClient c = asyncHttpClient()) {
            WebSocket webSocket = c.prepareGet("ws://localhost:" + port + "/")
                    .setHeader(SEC_WEBSOCKET_PROTOCOL, "chat, superchat")
                    .execute(upgradeHandler(openLatch)).get();

            assertTrue(openLatch.await(5, TimeUnit.SECONDS), "onOpen must fire when the server picks an offered subprotocol");
            assertEquals("superchat", webSocket.getUpgradeHeaders().get(SEC_WEBSOCKET_PROTOCOL));
        }
    }
}
