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
package org.asynchttpclient.cookie;

import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import org.asynchttpclient.uri.Uri;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * RFC 6265bis Section 5.7 cookie prefixes: __Secure- needs the Secure attribute, __Host- also needs to be
 * host-only with Path=/.
 */
public class CookiePrefixTest {

    private static void set(ThreadSafeCookieStore store, String url, String setCookie) {
        store.add(Uri.create(url), ClientCookieDecoder.LAX.decode(setCookie));
    }

    private static String sent(ThreadSafeCookieStore store, String url, String name) {
        return store.get(Uri.create(url)).stream().filter(c -> name.equals(c.name())).map(Cookie::value)
                .findFirst().orElse(null);
    }

    @Test
    public void aSiblingCannotPlantAHostCookieWithDomain() {
        ThreadSafeCookieStore store = new ThreadSafeCookieStore();
        set(store, "https://evil.example.com/", "__Host-SID=planted; Secure; Path=/; Domain=example.com");

        assertNull(sent(store, "https://example.com/", "__Host-SID"));
    }

    @Test
    public void aSiblingCannotDeleteAHostCookie() {
        ThreadSafeCookieStore store = new ThreadSafeCookieStore();
        set(store, "https://example.com/", "__Host-SID=real; Secure; Path=/");
        set(store, "https://evil.example.com/", "__Host-SID=x; Secure; Path=/; Domain=example.com; Max-Age=0");

        assertEquals("real", sent(store, "https://example.com/", "__Host-SID"));
    }

    @Test
    public void aHostCookieNeedsSecureAndRootPath() {
        ThreadSafeCookieStore store = new ThreadSafeCookieStore();
        set(store, "https://example.com/app/", "__Host-A=1; Path=/");
        set(store, "https://example.com/app/", "__Host-B=1; Secure");
        set(store, "https://example.com/app/", "__Host-C=1; Secure; Path=/app");
        set(store, "https://example.com/app/", "__host-D=1; Secure; Path=/; Domain=example.com");

        assertEquals(0, store.getAll().size());
    }

    @Test
    public void aSecurePrefixNeedsTheSecureAttribute() {
        ThreadSafeCookieStore store = new ThreadSafeCookieStore();
        set(store, "http://example.com/", "__Secure-SID=planted; Path=/");
        set(store, "https://example.com/", "__SECURE-X=1; Path=/");

        assertNull(sent(store, "https://example.com/", "__Secure-SID"));
        assertEquals(0, store.getAll().size());
    }

    @Test
    public void wellFormedPrefixedCookiesAreKept() {
        ThreadSafeCookieStore store = new ThreadSafeCookieStore();
        set(store, "https://example.com/", "__Host-SID=h; Secure; Path=/");
        set(store, "https://www.example.com/", "__Secure-T=s; Secure; Domain=example.com; Path=/");

        assertEquals("h", sent(store, "https://example.com/", "__Host-SID"));
        assertEquals("s", sent(store, "https://example.com/", "__Secure-T"));
    }
}
