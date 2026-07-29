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
package org.asynchttpclient.netty.channel;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.Utf8FrameValidator;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.asynchttpclient.Dsl.config;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The WebSocket receive pipeline must reject inbound text frames whose payload is not valid UTF-8
 * (RFC 6455 §8.1), matching what Netty's own WebSocketClientProtocolHandler does by default.
 */
class WebSocketUtf8ValidationTest {

    private ChannelManager channelManager;
    private Timer timer;

    @BeforeEach
    void setUp() {
        AsyncHttpClientConfig cfg = config().build();
        timer = new HashedWheelTimer();
        channelManager = new ChannelManager(cfg, timer);
    }

    @AfterEach
    void tearDown() {
        if (channelManager != null) {
            channelManager.close();
        }
        if (timer != null) {
            timer.stop();
        }
    }

    private EmbeddedChannel upgradedWebSocketChannel() {
        EmbeddedChannel channel = new EmbeddedChannel();
        // Reproduce the pre-handshake websocket pipeline the wsBootstrap sets up: an HTTP codec that
        // upgradePipelineForWebSockets swaps out, and the application websocket handler it inserts before.
        channel.pipeline().addLast(ChannelManager.HTTP_CLIENT_CODEC, new HttpClientCodec());
        channel.pipeline().addLast(ChannelManager.AHC_WS_HANDLER, new ChannelInboundHandlerAdapter());
        channelManager.upgradePipelineForWebSockets(channel.pipeline());
        return channel;
    }

    @Test
    void utf8ValidatorIsInstalled() {
        EmbeddedChannel channel = upgradedWebSocketChannel();
        assertNotNull(channel.pipeline().get(Utf8FrameValidator.class));
        channel.finishAndReleaseAll();
    }

    @Test
    void invalidUtf8TextFrameFailsTheConnection() {
        EmbeddedChannel channel = upgradedWebSocketChannel();
        // 0xFF is never a valid UTF-8 byte.
        TextWebSocketFrame frame = new TextWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{(byte) 0xFF}));
        assertThrows(CorruptedWebSocketFrameException.class, () -> channel.writeInbound(frame));
        channel.finishAndReleaseAll();
    }

    @Test
    void validUtf8TextFramePassesThrough() {
        EmbeddedChannel channel = upgradedWebSocketChannel();
        TextWebSocketFrame frame = new TextWebSocketFrame("héllo");
        assertTrue(channel.writeInbound(frame));
        channel.finishAndReleaseAll();
    }
}
