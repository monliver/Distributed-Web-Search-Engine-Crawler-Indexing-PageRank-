package webserver;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Server {

    public static final int NUM_WORKERS = 100;

    private static final ThreadLocal<SimpleDateFormat> RFC1123 =
            ThreadLocal.withInitial(() -> {
                SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
                fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
                return fmt;
            });

    private static Server INSTANCE = null;
    private static volatile boolean started = false;
    private static int configuredPort = 80;
    private static int securePort = -1;

    public static final Map<String, SessionImpl> sessions =
            Collections.synchronizedMap(new HashMap<>());

    private static final String DEFAULT_HOST = "__default__";
    private static String currentHost = DEFAULT_HOST;
    private static final Map<String, HostCtx> HOSTS = new HashMap<>();

    private static class HostCtx {
        final List<RouteEntry> routes = Collections.synchronizedList(new ArrayList<>());
        String staticRoot = null;
        final List<Route> beforeFilters = new ArrayList<>();
        final List<Route> afterFilters = new ArrayList<>();
    }

    private static class RouteEntry {
        final String method;
        final String pattern;
        final Route handler;
        RouteEntry(String m, String p, Route h) { method = m; pattern = p; handler = h; }
    }

    private static HostCtx ctx(String key) {
        return HOSTS.computeIfAbsent(key, k -> new HostCtx());
    }

    private static HostCtx regCtx() {
        return ctx(currentHost);
    }

    // --- Startup helpers ---
    private static synchronized void ensureInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Server();
        }
    }

    private static void ensureStarted() {
        if (!started) {
            synchronized (Server.class) {
                if (!started) {
                    started = true;
                    Thread t = new Thread(() -> INSTANCE.run(), "hw3-acceptor");
                    t.start();
                }
            }
        }
    }

    // --- Public API ---
    public static void port(int p) {
        configuredPort = p;
        ensureInstance();
    }

    public static void securePort(int p) {
        ensureInstance();
        securePort = p;
        ensureStarted();
    }

    // Extra credit: host with SNI keystore
    public static void host(String h, String keystoreFile, String password) {
        ensureInstance();
        if (h == null) {
            currentHost = DEFAULT_HOST;
        } else {
            String key = h.toLowerCase(Locale.US);
            int c = key.indexOf(':');
            currentHost = (c >= 0) ? key.substring(0, c) : key;
            try {
                SNIHelper.addKeystore(currentHost, keystoreFile, password);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void before(Route f) {
        ensureInstance();
        regCtx().beforeFilters.add(f);
        ensureStarted();
    }

    public static void after(Route f) {
        ensureInstance();
        regCtx().afterFilters.add(f);
        ensureStarted();
    }

    public static void get(String pattern, Route h) { addRoute("GET", pattern, h); }
    public static void post(String pattern, Route h) { addRoute("POST", pattern, h); }
    public static void put(String pattern, Route h)  { addRoute("PUT", pattern, h); }
    public static void delete(String pattern, Route h) { addRoute("DELETE", pattern, h); }

    private static void addRoute(String method, String pattern, Route h) {
        ensureInstance();
        regCtx().routes.add(new RouteEntry(method, pattern, h));
        ensureStarted();
    }

    public static class staticFiles {
        public static void location(String dir) {
            ensureInstance();
            regCtx().staticRoot = dir;
            ensureStarted();
        }
    }

    // --- Server runtime ---
    private void run() {
        BlockingQueue<Socket> queue = new LinkedBlockingQueue<>();
        startWorkers(queue);

        // HTTP listener
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(configuredPort)) {
                while (true) {
                    Socket client = serverSocket.accept();
                    queue.put(client);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "http-listener").start();

        // HTTPS listener (required for HW3)
        if (securePort > 0) {
            new Thread(() -> {
                try {
                    char[] pass = "secret".toCharArray();
                    java.security.KeyStore ks = java.security.KeyStore.getInstance("JKS");
                    try (FileInputStream fis = new FileInputStream("keystore.jks")) {
                        ks.load(fis, pass);
                    }

                    javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance("SunX509");
                    kmf.init(ks, pass);

                    javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("TLS");
                    sc.init(kmf.getKeyManagers(), null, null);

                    javax.net.ssl.SSLServerSocketFactory ssf = sc.getServerSocketFactory();
                    javax.net.ssl.SSLServerSocket sss =
                            (javax.net.ssl.SSLServerSocket) ssf.createServerSocket(securePort);

                    while (true) {
                        Socket client = sss.accept();
                        queue.put(client);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "https-listener").start();
        }
    }

    private static void startWorkers(BlockingQueue<Socket> queue) {
        for (int i = 0; i < NUM_WORKERS; i++) {
            Thread t = new Thread(() -> {
                while (true) {
                    try {
                        Socket s = queue.take();
                        INSTANCE.handleClient(s);
                    } catch (InterruptedException ie) {}
                }
            }, "worker-" + i);
            t.setDaemon(true);
            t.start();
        }
    }

    // --- Client handler ---
    private void handleClient(Socket socket) {
        try (
            InputStream rawIn = socket.getInputStream();
            OutputStream rawOut = socket.getOutputStream();
            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(rawOut, StandardCharsets.ISO_8859_1), true)
        ) {
            while (true) {
                byte[] headerBytes = readHeaders(rawIn);
                if (headerBytes == null) break;
                if (headerBytes.length == 0) continue;

                RequestHead req = parseRequest(headerBytes);
                if (req == null) {
                    sendError(out, 400, "Bad Request");
                    continue;
                }

                String hostHdr = req.headers.get("host");
                String hostKey = DEFAULT_HOST;
                if (hostHdr != null) {
                    String hk = hostHdr.toLowerCase(Locale.US).trim();
                    int c = hk.indexOf(':');
                    hostKey = (c >= 0) ? hk.substring(0, c) : hk;
                }
                HostCtx hostCtx = HOSTS.getOrDefault(hostKey, HOSTS.get(DEFAULT_HOST));
                if (hostCtx == null) hostCtx = new HostCtx();

                String method = req.method;
                String rawTarget = req.path;
                String pathOnly = rawTarget;
                String rawQuery = null;
                int qidx = rawTarget.indexOf('?');
                if (qidx >= 0) {
                    pathOnly = rawTarget.substring(0, qidx);
                    rawQuery = rawTarget.substring(qidx + 1);
                }

                byte[] bodyBytes = new byte[0];
                int contentLength = 0;
                String cl = req.headers.get("content-length");
                if (cl != null) {
                    try { contentLength = Integer.parseInt(cl); } catch (Exception ignore) {}
                }
                if (contentLength > 0) {
                    bodyBytes = new byte[contentLength];
                    int read = 0;
                    while (read < contentLength) {
                        int n = rawIn.read(bodyBytes, read, contentLength - read);
                        if (n < 0) break;
                        read += n;
                    }
                }

                Map<String,String> qparams = parseQueryParams(rawQuery);
                String ctype = req.headers.get("content-type");
                if (ctype != null && ctype.startsWith("application/x-www-form-urlencoded") && bodyBytes.length > 0) {
                    String bodyStr = new String(bodyBytes, StandardCharsets.UTF_8);
                    Map<String,String> extra = parseQueryParams(bodyStr);
                    qparams.putAll(extra);
                }

                RouteMatch match = findMatch(hostCtx.routes, method, pathOnly);
                if (match != null) {
                    ResponseImpl response = new ResponseImpl(rawOut);
                    Request request = new RequestImpl(
                        method, pathOnly, req.version, req.headers, qparams,
                        match.pathParams, (InetSocketAddress) socket.getRemoteSocketAddress(),
                        bodyBytes, response
                    );

                    for (Route f : hostCtx.beforeFilters) {
                        try { f.handle(request, response); } catch (Exception ignore) {}
                        if (response.isHalted()) {
                            if (!response.headersSent()) response.commit();
                            return;
                        }
                    }

                    Object ret = null;
                    try {
                        ret = match.entry.handler.handle(request, response);
                    } catch (Exception ex) {
                        if (!response.headersSent()) {
                            sendError(out, 500, "Internal Server Error");
                        }
                        return;
                    }

                    for (Route f : hostCtx.afterFilters) {
                        try { f.handle(request, response); } catch (Exception ignore) {}
                    }

                    if (response.headersSent()) {
                        rawOut.flush();
                        return;
                    }

                    if (ret instanceof String) {
                        response.body((String) ret);
                    } else if (ret instanceof byte[]) {
                        response.bodyAsBytes((byte[]) ret);
                    } else if (ret == null) {
                        response.bodyAsBytes(new byte[0]);
                    }
                    response.commit();
                    continue;
                }

                if (hostCtx.staticRoot == null) {
                    sendError(out, 404, "Not Found");
                    break;
                }
                serveStatic(req, out, rawOut, hostCtx.staticRoot);
            }
        } catch (IOException ioe) {
        } finally {
            try { socket.close(); } catch (IOException ignore) {}
        }
    }

    // --- Route matching ---
    private static class RouteMatch {
        RouteEntry entry;
        Map<String,String> pathParams;
    }

    private RouteMatch findMatch(List<RouteEntry> routeList, String method, String path) {
        String[] urlSegs = normalize(path).split("/");
        synchronized (routeList) {
            for (RouteEntry e : routeList) {
                if (!e.method.equalsIgnoreCase(method)) continue;
                String[] patSegs = normalize(e.pattern).split("/");
                if (urlSegs.length != patSegs.length) continue;

                Map<String,String> params = new HashMap<>();
                boolean ok = true;
                for (int i = 0; i < urlSegs.length; i++) {
                    String p = patSegs[i];
                    String u = urlSegs[i];
                    if (p.startsWith(":")) {
                        params.put(p.substring(1), u);
                    } else if (!p.equals(u)) {
                        ok = false; break;
                    }
                }
                if (ok) {
                    RouteMatch m = new RouteMatch();
                    m.entry = e; m.pathParams = params;
                    return m;
                }
            }
        }
        return null;
    }

    private static String normalize(String s) {
        if (s == null || s.isEmpty()) return "";
        if (s.charAt(0) == '/') s = s.substring(1);
        if (s.endsWith("/")) s = s.substring(0, s.length()-1);
        return s;
    }

    // --- Static file serving ---
    private void serveStatic(RequestHead req, PrintWriter out, OutputStream rawOut, String rootDir) {
        try {
            String p = req.path;
            int q = p.indexOf('?'); if (q >= 0) p = p.substring(0, q);
            int h = p.indexOf('#'); if (h >= 0) p = p.substring(0, h);

            String path = "/".equals(p) ? "/index.html" : p;
            if (path.contains("..")) {
                sendError(out, 403, "Forbidden");
                return;
            }
            String normalized = path.startsWith("/") ? path.substring(1) : path;
            File file = new File(rootDir, normalized);

            if (!file.exists()) {
                sendError(out, 404, "Not Found");
                return;
            }
            if (!file.canRead()) {
                sendError(out, 403, "Forbidden");
                return;
            }

            String contentType = getContentType(file.getName());
            long totalLen = file.length();
            long lastModMillis = file.lastModified();

            out.print("HTTP/1.1 200 OK\r\n");
            out.print("Content-Type: " + contentType + "\r\n");
            out.print("Content-Length: " + totalLen + "\r\n");
            out.print("Last-Modified: " + RFC1123.get().format(new Date(lastModMillis)) + "\r\n");
            out.print("Server: MySimpleServer\r\n");
            out.print("\r\n");
            out.flush();

            try (InputStream fis = new FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = fis.read(buf)) != -1) {
                    rawOut.write(buf, 0, n);
                }
                rawOut.flush();
            }
        } catch (Exception e) {
            sendError(out, 500, "Internal Server Error");
        }
    }

    // --- Parsing helpers ---
    private static Map<String,String> parseQueryParams(String raw) {
        Map<String,String> m = new HashMap<>();
        if (raw == null || raw.isEmpty()) return m;
        for (String kv : raw.split("&")) {
            int eq = kv.indexOf('=');
            String k = (eq < 0) ? kv : kv.substring(0, eq);
            String v = (eq < 0) ? ""  : kv.substring(eq + 1);
            try {
                k = java.net.URLDecoder.decode(k, "UTF-8");
                v = java.net.URLDecoder.decode(v, "UTF-8");
            } catch (Exception ignore) {}
            if (m.containsKey(k)) {
                m.put(k, m.get(k) + "," + v);
            } else {
                m.put(k, v);
            }
        }
        return m;
    }

    private static class RequestHead {
        String method;
        String path;
        String version;
        Map<String, String> headers;
    }

    private static byte[] readHeaders(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        int prev3 = -1, prev2 = -1, prev1 = -1, curr;
        boolean gotAny = false;
        while ((curr = in.read()) != -1) {
            gotAny = true;
            baos.write(curr);
            if (prev3 == 13 && prev2 == 10 && prev1 == 13 && curr == 10) {
                break;
            }
            prev3 = prev2;
            prev2 = prev1;
            prev1 = curr;
        }
        if (!gotAny && curr == -1 && baos.size() == 0) {
            return null;
        }
        return baos.toByteArray();
    }

    private static RequestHead parseRequest(byte[] headerBytes) throws IOException {
        String head = new String(headerBytes, "ISO-8859-1");
        String[] lines = head.split("\r\n");
        if (lines.length == 0 || lines[0].isEmpty()) return null;

        String[] p = lines[0].split(" ", 3);
        if (p.length != 3) return null;

        RequestHead req = new RequestHead();
        req.method = p[0];
        req.path = p[1];
        req.version = p[2];
        req.headers = new HashMap<>();

        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            if (line.isEmpty()) break;
            int idx = line.indexOf(':');
            if (idx > 0) {
                String key = line.substring(0, idx).trim().toLowerCase();
                String val = line.substring(idx + 1).trim();
                req.headers.put(key, val);
            }
        }
        return req;
    }

    private static void sendError(PrintWriter out, int code, String message) {
        String body = code + " " + message;
        out.print("HTTP/1.1 " + code + " " + message + "\r\n");
        out.print("Content-Type: text/plain\r\n");
        out.print("Content-Length: " + body.length() + "\r\n");
        out.print("Server: MySimpleServer\r\n");
        out.print("\r\n");
        out.print(body);
        out.flush();
    }

    private static String getContentType(String name) {
        String lower = name.toLowerCase(Locale.US);
        if (lower.endsWith(".html") || lower.endsWith(".htm")) return "text/html";
        if (lower.endsWith(".txt"))  return "text/plain";
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
        if (lower.endsWith(".css")) return "text/css";
        if (lower.endsWith(".js")) return "application/javascript"; 
        return "application/octet-stream";
    }
}
