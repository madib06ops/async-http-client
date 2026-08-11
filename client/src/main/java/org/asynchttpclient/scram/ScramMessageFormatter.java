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
package org.asynchttpclient.scram;

import org.jetbrains.annotations.Nullable;

import java.util.Base64;

/**
 * Formats SCRAM protocol messages (RFC 5802) and HTTP headers (RFC 7804).
 * Thread-safe: all methods are stateless.
 */
public final class ScramMessageFormatter {

    private ScramMessageFormatter() {
    }

    /**
     * Escape a username per RFC 5802: "=" → "=3D", "," → "=2C".
     */
    public static String escapeUsername(String username) {
        // Order matters: escape '=' first to avoid double-escaping
        return username.replace("=", "=3D").replace(",", "=2C");
    }

    /**
     * Format the bare portion of client-first-message (without gs2-header).
     * "n=&lt;escaped-user&gt;,r=&lt;c-nonce&gt;"
     */
    public static String clientFirstMessageBare(String username, String clientNonce) {
        return "n=" + escapeUsername(username) + ",r=" + clientNonce;
    }

    /**
     * Format the full client-first-message including gs2-header.
     * "n,,n=&lt;escaped-user&gt;,r=&lt;c-nonce&gt;"
     */
    public static String formatClientFirstMessage(String username, String clientNonce) {
        return "n,," + clientFirstMessageBare(username, clientNonce);
    }

    /**
     * Format client-final-message-without-proof.
     * "c=biws,r=&lt;full-nonce&gt;"
     */
    public static String clientFinalMessageWithoutProof(String fullNonce) {
        return "c=biws,r=" + fullNonce;
    }

    /**
     * Format the full client-final-message with proof.
     * "c=biws,r=&lt;full-nonce&gt;,p=&lt;base64-proof&gt;"
     */
    public static String formatClientFinalMessage(String fullNonce, byte[] clientProof) {
        return clientFinalMessageWithoutProof(fullNonce) + ",p=" + Base64.getEncoder().encodeToString(clientProof);
    }

    /**
     * Format the HTTP Authorization header value for SCRAM.
     * Per Erratum 6558, the data attribute is quoted.
     *
     * @param mechanism  "SCRAM-SHA-256"
     * @param realm      realm value (may be null)
     * @param sid        session ID (may be null)
     * @param base64Data base64-encoded SCRAM message
     * @return formatted header value
     */
    public static String formatAuthorizationHeader(String mechanism, @Nullable String realm,
                                                    @Nullable String sid, String base64Data) {
        StringBuilder sb = new StringBuilder(mechanism);
        if (realm != null) {
            sb.append(" realm=\"").append(requireSafeChallengeValue("realm", realm, true)).append("\",");
        }
        if (sid != null) {
            sb.append(" sid=").append(requireSafeChallengeValue("sid", sid, false)).append(",");
        }
        sb.append(" data=\"").append(base64Data).append("\"");
        return sb.toString();
    }

    /**
     * Reject a server-supplied challenge value before it is echoed into the credentials header. {@code realm}
     * and {@code sid} come from the SCRAM {@code WWW-Authenticate} / {@code Proxy-Authenticate} challenge (RFC
     * 7804 §4): realm is written as a quoted-string and sid as a bare token. A double quote or backslash closes
     * (or escapes past) the quoted realm, and a comma or space ends the unquoted sid, so a hostile server could
     * append forged auth-params — a second {@code data} for an intermediary to parse — to the {@code
     * Authorization} the client emits; a CR or LF splits the header outright. An RFC 7235 realm and the base64
     * sid never legitimately contain these, so fail closed rather than serialize an ambiguous header.
     */
    private static String requireSafeChallengeValue(String name, String value, boolean quoted) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < ' ' || c == 0x7f || c == '"' || c == '\\' || (!quoted && (c == ',' || c == ' '))) {
                throw new IllegalArgumentException("Illegal character in SCRAM " + name + " challenge value");
            }
        }
        return value;
    }
}
