package webserver;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

/**
 * Implementation of the Request interface for HW3.
 * Handles request line, headers, params, body, and session handling.
 */
public class RequestImpl implements Request {
    private final String method;
    private final String url;             // path only (no query string)
    private final String protocol;
    private final InetSocketAddress remoteAddr;
    private final Map<String, String> headers;
    private final Map<String, String> queryParams;
    private final Map<String, String> pathParams;
    private final byte[] bodyRaw;

    // --- Session state ---
    private SessionImpl sessionObj = null;   // cached session
    private final ResponseImpl responseRef;  // needed for Set-Cookie

    public RequestImpl(String methodArg,
                       String urlArg,
                       String protocolArg,
                       Map<String, String> headersArg,
                       Map<String, String> queryParamsArg,
                       Map<String, String> pathParamsArg,
                       InetSocketAddress remoteAddrArg,
                       byte[] bodyRawArg,
                       ResponseImpl responseRefArg) {
        this.method = methodArg;
        this.url = urlArg;
        this.protocol = protocolArg;
        this.headers = headersArg;
        this.queryParams = queryParamsArg;
        this.pathParams = pathParamsArg;
        this.remoteAddr = remoteAddrArg;
        this.bodyRaw = (bodyRawArg == null ? new byte[0] : bodyRawArg);
        this.responseRef = responseRefArg;
    }

    // --- Client info ---
    @Override
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }

    @Override
    public int port() {
        return remoteAddr.getPort();
    }

    // --- Request line ---
    @Override
    public String requestMethod() {
        return method;
    }

    @Override
    public String url() {
        return url;
    }

    @Override
    public String protocol() {
        return protocol;
    }

    // --- Headers ---
    @Override
    public Set<String> headers() {
        return headers.keySet();
    }

    @Override
    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }

    @Override
    public String contentType() {
        return headers.get("content-type");
    }

    // --- Body ---
    @Override
    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] bodyAsBytes() {
        return bodyRaw;
    }

    @Override
    public int contentLength() {
        return bodyRaw.length;
    }

    // --- Query params ---
    @Override
    public Set<String> queryParams() {
        return queryParams.keySet();
    }

    @Override
    public String queryParams(String param) {
        return queryParams.get(param);
    }

    // --- Path params ---
    @Override
    public Map<String, String> params() {
        return pathParams;
    }

    @Override
    public String params(String name) {
        return pathParams.get(name);
    }

    // --- HW3: Session handling ---
    @Override
    public Session session() {
        if (sessionObj != null) return sessionObj;

        String cookieHeader = headers.get("cookie");
        String existingId = null;

        // Parse cookies for SessionID
        if (cookieHeader != null) {
            for (String part : cookieHeader.split(";")) {
                String[] kv = part.trim().split("=", 2);
                if (kv.length == 2 && kv[0].equals("SessionID")) {
                    existingId = kv[1];
                    break;
                }
            }
        }

        synchronized (Server.sessions) {
            if (existingId != null) {
                SessionImpl s = Server.sessions.get(existingId);
                if (s != null && !s.expired() && s.isValid()) {
                    s.access();
                    sessionObj = s;
                    return sessionObj;
                }
                if (s != null) s.invalidate();
                Server.sessions.remove(existingId);
            }

            // Always create a new session if none valid
            String newId = randomSessionId();
            SessionImpl ns = new SessionImpl(newId);
            Server.sessions.put(newId, ns);
            sessionObj = ns;

            // Add Set-Cookie header
            if (responseRef != null) {
                StringBuilder sb = new StringBuilder();
                sb.append("SessionID=").append(newId);
                sb.append("; Path=/");
                sb.append("; HttpOnly");
                sb.append("; SameSite=Lax");

                responseRef.header("Set-Cookie", sb.toString());
            }
            return sessionObj;
        }
    }

    // Generate random session ID with ~120 bits of entropy
    private static String randomSessionId() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        SecureRandom rnd = new SecureRandom();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }
        return sb.toString();
    }
}
