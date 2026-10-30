/*
 *    Copyright (c) 2014-2024 AsyncHttpClient Project. All rights reserved.
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
package org.asynchttpclient.netty.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.asynchttpclient.AsyncHandler.State;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.netty.NettyResponseFuture;
import org.asynchttpclient.netty.NettyResponseStatus;
import org.asynchttpclient.netty.channel.ChannelManager;
import org.asynchttpclient.netty.channel.Channels;
import org.asynchttpclient.netty.request.NettyRequestSender;
import org.asynchttpclient.netty.ws.NettyWebSocket;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.io.IOException;
import java.util.List;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS;
import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_KEY;
import static io.netty.handler.codec.http.HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL;
import static io.netty.handler.codec.http.HttpHeaderNames.UPGRADE;
import static io.netty.handler.codec.http.HttpResponseStatus.SWITCHING_PROTOCOLS;
import static org.asynchttpclient.netty.channel.ChannelManager.WS_DECODER_HANDLER;
import static org.asynchttpclient.netty.channel.ChannelManager.WS_FRAME_AGGREGATOR;
import static org.asynchttpclient.util.MiscUtils.getCause;
import static org.asynchttpclient.ws.WebSocketUtils.getAcceptKey;

@Sharable
public final class WebSocketHandler extends AsyncHttpClientHandler {

    public WebSocketHandler(AsyncHttpClientConfig config, ChannelManager channelManager, NettyRequestSender requestSender) {
        super(config, channelManager, requestSender);
    }

    private static WebSocketUpgradeHandler getWebSocketUpgradeHandler(NettyResponseFuture<?> future) {
        return (WebSocketUpgradeHandler) future.getAsyncHandler();
    }

    private static NettyWebSocket getNettyWebSocket(NettyResponseFuture<?> future) throws Exception {
        return getWebSocketUpgradeHandler(future).onCompleted();
    }

    private void upgrade(Channel channel, NettyResponseFuture<?> future, WebSocketUpgradeHandler handler, HttpResponse response, HttpHeaders responseHeaders) throws Exception {
        boolean validStatus = response.status().equals(SWITCHING_PROTOCOLS);
        // RFC 6455 section 4.1 step 2: the value has to be websocket, not merely present.
        boolean validUpgrade = HttpHeaderValues.WEBSOCKET.contentEqualsIgnoreCase(response.headers().get(UPGRADE));
        String connection = response.headers().get(CONNECTION);
        boolean validConnection = HttpHeaderValues.UPGRADE.contentEqualsIgnoreCase(connection);
        final boolean headerOK = handler.onHeadersReceived(responseHeaders) == State.CONTINUE;
        if (!headerOK || !validStatus || !validUpgrade || !validConnection) {
            requestSender.abort(channel, future, new IOException("Invalid handshake response"));
            return;
        }

        HttpHeaders requestHeaders = future.getNettyRequest().getHttpRequest().headers();
        String accept = response.headers().get(SEC_WEBSOCKET_ACCEPT);
        String key = getAcceptKey(requestHeaders.get(SEC_WEBSOCKET_KEY));
        if (accept == null || !accept.equals(key)) {
            requestSender.abort(channel, future, new IOException("Invalid challenge. Actual: " + accept + ". Expected: " + key));
            return;
        }

        // RFC 6455 section 4.1 steps 5 and 6: the accept key only proves the peer saw the key, it does not bind
        // what was negotiated, so a subprotocol or extension the request did not offer fails the connection.
        String subprotocol = response.headers().get(SEC_WEBSOCKET_PROTOCOL);
        if (subprotocol != null && !offered(requestHeaders.getAll(SEC_WEBSOCKET_PROTOCOL), subprotocol)) {
            requestSender.abort(channel, future, new IOException("Invalid subprotocol. Actual: " + subprotocol
                    + ". Expected one of: " + requestHeaders.getAll(SEC_WEBSOCKET_PROTOCOL)));
            return;
        }
        String extensions = response.headers().get(SEC_WEBSOCKET_EXTENSIONS);
        if (extensions != null && !extensions.trim().isEmpty() && !requestHeaders.contains(SEC_WEBSOCKET_EXTENSIONS)) {
            requestSender.abort(channel, future, new IOException("Invalid extensions. Actual: " + extensions + ". None offered"));
            return;
        }

        // set back the future so the protocol gets notified of frames
        // removing the HttpClientCodec from the pipeline might trigger a read with a WebSocket message
        // if it comes in the same frame as the HTTP Upgrade response
        Channels.setAttribute(channel, future);

        final NettyWebSocket webSocket = new NettyWebSocket(channel, responseHeaders);
        handler.setWebSocket(webSocket);
        channelManager.upgradePipelineForWebSockets(channel.pipeline());

        // We don't need to synchronize as replacing the "ws-decoder" will
        // process using the same thread.
        try {
            handler.onOpen(webSocket);
        } catch (Exception ex) {
            logger.warn("onSuccess unexpected exception", ex);
        }
        future.done();
    }

    // Subprotocol names are case-sensitive tokens; the request may spread its offers over several header fields.
    private static boolean offered(List<String> offers, String selected) {
        String token = selected.trim();
        for (String offer : offers) {
            for (String candidate : offer.split(",")) {
                if (candidate.trim().equals(token)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void abort(Channel channel, NettyResponseFuture<?> future, WebSocketUpgradeHandler handler, HttpResponseStatus status) {
        try {
            handler.onThrowable(new IOException("Invalid Status code=" + status.getStatusCode() + " text=" + status.getStatusText()));
        } finally {
            finishUpdate(future, channel, true);
        }
    }

    @Override
    public void handleRead(Channel channel, NettyResponseFuture<?> future, Object e) throws Exception {

        if (e instanceof HttpResponse) {
            // Unlike HttpHandler, this check cannot guard the whole method: a successful upgrade completes
            // the future (see upgrade() below) and the channel then keeps serving frames, so isDone() is
            // true for all normal WebSocket traffic. It belongs on the upgrade response alone, where a 101
            // arriving just as a request timeout aborted the future would otherwise still run onOpen and
            // deliver the WebSocket lifecycle after onThrowable had already fired.
            if (future.isDone()) {
                channelManager.closeChannel(channel);
                return;
            }

            HttpResponse response = (HttpResponse) e;
            if (logger.isDebugEnabled()) {
                HttpRequest httpRequest = future.getNettyRequest().getHttpRequest();
                logger.debug("\n\nRequest {}\n\nResponse {}\n", HttpMessageFormatter.format(httpRequest), HttpMessageFormatter.format(response));
            }

            WebSocketUpgradeHandler handler = getWebSocketUpgradeHandler(future);
            HttpResponseStatus status = new NettyResponseStatus(future.getUri(), response, channel);
            HttpHeaders responseHeaders = response.headers();

            if (!interceptors.exitAfterIntercept(channel, future, handler, response, status, responseHeaders)) {
                if (handler.onStatusReceived(status) == State.CONTINUE) {
                    upgrade(channel, future, handler, response, responseHeaders);
                } else {
                    abort(channel, future, handler, status);
                }
            }

        } else if (e instanceof WebSocketFrame) {
            WebSocketFrame frame = (WebSocketFrame) e;
            NettyWebSocket webSocket = getNettyWebSocket(future);
            // retain because we might buffer the frame
            if (webSocket.isReady()) {
                webSocket.handleFrame(frame);
            } else {
                // WebSocket hasn't been opened yet, but upgrading the pipeline triggered a read and a frame was sent along the HTTP upgrade response
                // as we want to keep sequential order (but can't notify user of open before upgrading, so he doesn't try to send immediately), we have to buffer
                webSocket.bufferFrame(frame);
            }

        } else if (!(e instanceof LastHttpContent)) {
            // ignore, end of handshake response
            logger.error("Invalid message {}", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        // RFC 6455 section 7.4.1: 1009 for a message too big to process, whether the aggregator refused it or the
        // inflater did. Queued before the close below.
        Throwable cause = getCause(e);
        if ((cause instanceof TooLongFrameException && ctx.pipeline().get(WS_FRAME_AGGREGATOR) != null)
                || (cause instanceof DecompressionException && ctx.pipeline().get(WS_DECODER_HANDLER) != null)) {
            ctx.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.MESSAGE_TOO_BIG));
        }
        super.exceptionCaught(ctx, e);
    }

    @Override
    public void handleException(NettyResponseFuture<?> future, Throwable e) {
        logger.warn("onError", e);

        try {
            NettyWebSocket webSocket = getNettyWebSocket(future);
            if (webSocket != null) {
                webSocket.onError(e);
                webSocket.sendCloseFrame();
            }
        } catch (Throwable t) {
            logger.error("onError", t);
        }
    }

    @Override
    public void handleChannelInactive(NettyResponseFuture<?> future) {
        logger.trace("Connection was closed abnormally (that is, with no close frame being received).");

        try {
            NettyWebSocket webSocket = getNettyWebSocket(future);
            if (webSocket != null) {
                webSocket.onClose(1006, "Connection was closed abnormally (that is, with no close frame being received).");
            }
        } catch (Throwable t) {
            logger.error("onError", t);
        }
    }
}
