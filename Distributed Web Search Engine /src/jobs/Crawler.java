package jobs;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.net.ssl.SSLHandshakeException;

import flame.FlameContext;
import flame.FlameRDD;
import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;
import tools.URLParser;


public class Crawler {
    //constants, default seed URL and UA
    private static final int TARGET_HTML_PAGES  = 50000;
    private static final int MAX_PAGES_PER_HOST = 500;
    private static final int MAX_BYTES_PER_PAGE = 100_000;
    private static final int MAX_ROBOTS_BYTES = 64 * 1024;

    private static final double DEFAULT_DELAY = 1.0;
    private static final String[] DEFAULT_SEEDS = Urls.DEFAULT_SEEDS;
    private static final String UA = "cis5550-crawler";

    private static final Set<String> BLACKLISTED_HOSTS = Urls.BLACKLISTED_HOSTS;
    private static final List<String> BLACKLISTED_SUBSTRINGS = Urls.BLACKLISTED_SUBSTRINGS;
    private static final List<String> BLACKLISTED_EXTENSIONS = Urls.BLACKLISTED_EXTENSIONS;
    private static final List<String> BLACKLISTED_LANGUAGE = Urls.BLACKLISTED_LANGUAGE;

    //counter
    private static final java.util.concurrent.ConcurrentMap<String,Integer> hostCounts =
    new java.util.concurrent.ConcurrentHashMap<>();
    
    public static void run(FlameContext context, String[] args) throws Exception {


        //get raw seeds from args
        List<String> rawSeeds = new ArrayList<>();
        if (args != null && args.length > 0) {
            for (String a : args) {
                if (a != null && !a.isBlank()) {
                    rawSeeds.add(a.trim());
                }
            }
        }

        // if no raw seeds were passed, use DEFAULT_SEEDS
        if (rawSeeds.isEmpty()) {
            rawSeeds.addAll(Arrays.asList(DEFAULT_SEEDS));
        }

        //normalize seeds
        KVSClient admin = context.getKVS();

        List<String> normalizedSeeds = new ArrayList<>();
        for (String s : rawSeeds) {
            String normalizedSeed = normalizeSeedUrl(s);
            if (normalizedSeed != null && !normalizedSeeds.contains(normalizedSeed)) {
                normalizedSeeds.add(normalizedSeed);
            }
        }

        if (normalizedSeeds.isEmpty()) {
            context.output("Error: all seed URLs invalid after normalization");
            return;
        }


        FlameRDD urlQueue = null;
        //KVSClient admin = context.getKVS();
        final String kvsCoord = admin.getCoordinator();

        // Count the current number of crawled pages
        int currentCrawlCount = admin.count("pt-crawl");

        // Check if the pt-crawl-queue table exists and has pending URLs
        int pendingUrlsCount = 0;
        try {
            pendingUrlsCount = admin.count("pt-crawl-queue");
        } catch (Exception e) {
            context.output("pt-crawl-queue table does not exist. Starting fresh crawl...");
        }
        // if first run, put normalizedSeeds into pt-crawl-queue
        if (pendingUrlsCount == 0) { 
            for (String s : normalizedSeeds) {
                admin.put("pt-crawl-queue", Hasher.hash(s), "value", s);
            }
            pendingUrlsCount = normalizedSeeds.size();
        }

        // Initialize the URL queue
        if (pendingUrlsCount > 0) {
            List<String> pendingUrls = new ArrayList<>();
            Iterator<kvs.Row> iterator = admin.scan("pt-crawl-queue");
            while (iterator.hasNext()) {
                kvs.Row row = iterator.next();
                String url = row.get("value");
                if (url == null || url.isBlank()) {
                    continue;
                }
                String key = Hasher.hash(url);
                if (!admin.existsRow("pt-crawl", key)){
                    pendingUrls.add(url);
                } else {
                    try {
                        admin.deleteRow("pt-crawl-queue", Hasher.hash(url));
                    } catch (Exception ignored) { }
                }
            }
            urlQueue = context.parallelize(pendingUrls);
        }

        int i = 0;
        
        while (true) {
            KVSClient guard = context.getKVS();

            if (reachedTargetCrawl(guard, TARGET_HTML_PAGES)) {
                break;
            }

            try {

                if (urlQueue.count() == 0) {
                    FlameRDD newUrlQueue = context.fromTable("pt-crawl-queue", row -> row.get("value"));
                    if (newUrlQueue.count() == 0) {
                        break;
                    }
                    urlQueue = newUrlQueue;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                i++;
                
                //flatMap
                urlQueue = urlQueue.flatMap(url -> {
                    try {
                        KVSClient kvs = new KVSClient(kvsCoord);
                        String key = Hasher.hash(url);
                        boolean completed = false;
                        boolean throttled = false;


                        try {
                            try {
                                String host = hostname(url);
                                if (host == null || host.isBlank()) {
                                    throw new IllegalArgumentException("hostname unresolved");
                                }
                                if (!underHostCap(url)) {
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }
                                //url = normalized;

                            } catch (Exception e) {
                                completed = true;
                                return java.util.Collections.<String>emptyList();
                            }

                            List<String> newUrls = new ArrayList<>();
                            try {

                                String host = extractHostKey(url);
                                String protocol = extractProtocol(url);
                                String path = extractPath(url);

                                try {
                                    ensureRobotsFetched(kvs, protocol, host);
                                } catch (Exception e) {
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                if (isRecent(kvs, host, System.currentTimeMillis())) {
                                    throttled = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                try {
                                    boolean robotsAllowed = robotsAllows(kvs, host, path);

                                    if (!robotsAllowed) {
                                        updateLastAccess(kvs, host, System.currentTimeMillis());
                                        completed = true;
                                        return java.util.Collections.<String>emptyList();
                                    }
                                } catch (Exception e) {
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                //HEAD
                                HttpURLConnection head = null;
                                try {
                                    head = openConnectionAndConfigure(url, "HEAD", false, 5000, 7000, false);
                                    head.connect();

                                } catch (Exception e) {
                                    if (head != null) try {
                                        head.disconnect();
                                    } catch (Exception ignore) {
                                    }
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }
                                updateLastAccess(kvs, host, System.currentTimeMillis());

                                int headResponseCode;
                                String headMedia;
                                String headContentType;
                                String headLanguage;
                                try {
                                    headResponseCode = head.getResponseCode();
                                    headContentType = head.getContentType();
                                    headMedia = (headContentType == null) ? null : headContentType.split(";", 2)[0].trim();
                                    headLanguage = head.getHeaderField("Content-Language");

                                } catch (Exception e) {
                                    head.disconnect();
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                // skip non-English page
                                if (headLanguage != null && !headLanguage.toLowerCase().startsWith("en")) {
                                    completed = true;
                                    return Collections.emptyList();
                                }

                                if (headResponseCode == 301 || headResponseCode == 302 || headResponseCode == 303 ||
                                        headResponseCode == 307 || headResponseCode == 308) {
                                    String location = head.getHeaderField("Location");
                                    String target = normalizeAndFilterUrl(url, location);
                                    if (target != null && shouldCrawl(target)) {
                                        newUrls.add(target);
                                    }
                                    head.disconnect();
                                    completed = true;
                                    return newUrls;
                                }

                                if (headResponseCode != 200) {
                                    head.disconnect();
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                boolean isHtml = headMedia != null && headMedia.toLowerCase().contains("text/html");
                                if (!isHtml) {
                                    head.disconnect();
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                //GET
                                HttpURLConnection connection = null;
                                try {
                                    connection = openConnectionAndConfigure(url, "GET", true, 5000, 7000, true);
                                    connection.connect();
                                } catch (Exception e) {
                                    if (connection != null) try {
                                        connection.disconnect();
                                    } catch (Exception ignore) {
                                    }
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                updateLastAccess(kvs, host, System.currentTimeMillis());

                                int responseCode = connection.getResponseCode();

                                if (responseCode == 301 || responseCode == 302 || responseCode == 303 ||
                                        responseCode == 307 || responseCode == 308) {
                                    String location2 = connection.getHeaderField("Location");
                                    String target2 = normalizeAndFilterUrl(url, location2);
                                    if (target2 != null && shouldCrawl(target2)) {
                                        newUrls.add(target2);
                                    }
                                }

                                if (responseCode != 200) {
                                    connection.disconnect();
                                    completed = true;
                                    return java.util.Collections.<String>emptyList();
                                }

                                if (headMedia != null && headMedia.toLowerCase().contains("text/html")) {
                                    String getMedia = connection.getHeaderField("Content-Type");
                                    String contentType = (getMedia != null ? getMedia.split(";", 2)[0].trim() : headMedia);

                                    // size-capped GET read
                                    try (InputStream in = connection.getInputStream();
                                         java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(
                                                 Math.min(MAX_BYTES_PER_PAGE, 256_000))) {

                                        byte[] buf = new byte[32 * 1024];
                                        int read;
                                        int total = 0;

                                        while ((read = in.read(buf)) != -1) {
                                            int next = Math.min(read, MAX_BYTES_PER_PAGE - total);
                                            if (next > 0) {
                                                out.write(buf, 0, next);
                                                total += next;
                                            }
                                            if (total >= MAX_BYTES_PER_PAGE) break;
                                        }

                                        byte[] body = out.toByteArray();

                                        saveToTable(kvs, key, url, 200, contentType, body, "ok", true);
                                        connection.disconnect();

                                        // Count this page toward host cap and global target
                                        String hostName = hostname(url);
                                        hostCounts.merge(hostName, 1, Integer::sum);

                                        //extractURLs
                                        try {
                                            String bodyString = new String(body, StandardCharsets.UTF_8);
                                            newUrls = extractUrls(bodyString);

                                        } catch (Exception e) {
                                            connection.disconnect();
                                            return java.util.Collections.<String>emptyList();
                                        }

                                        Set<String> normalizedUrls = new HashSet<>();

                                        if (newUrls != null && !newUrls.isEmpty()) {
                                            for (String u : newUrls) {
                                                if (isSkippableHref(u)) {
                                                    continue;
                                                }
                                                u = unwrapUrlDefense(u);
                                                if (u == null) {
                                                    continue;
                                                }

                                                String normalizedUrl = normalizeAndFilterUrl(url, u);
                                                if (normalizedUrl == null) {
                                                    continue;
                                                }

                                                if (normalizedUrls.contains(normalizedUrl)) {
                                                    continue;
                                                }

                                                if (!shouldCrawl(normalizedUrl)) {
                                                    continue;
                                                }

                                                String targetKey = Hasher.hash(normalizedUrl);

                                                try {
                                                    if (kvs.existsRow("pt-crawl-visited", targetKey)) {
                                                        continue;
                                                    }
                                                } catch (Exception e) {
                                                    continue;
                                                }

                                                if (!underHostCap(normalizedUrl)) {
                                                    continue;
                                                }

                                                normalizedUrls.add(normalizedUrl);
                                                kvs.put("pt-crawl-queue", targetKey, "value", normalizedUrl);
                                            }

                                            newUrls.addAll(normalizedUrls);
                                        }
                                    } catch (Exception e) {
                                        connection.disconnect();
                                        //return newUrls;
                                        completed = true;
                                        return java.util.Collections.<String>emptyList();
                                    }
                                }
                                connection.disconnect();
                                completed = true;
                                return newUrls;

                            } catch (IOException e) {
                                completed = true;
                                return java.util.Collections.<String>emptyList();
                            }
                        } finally {
                            // delete url from pt-crawl-queue
                            try {
                                if (completed) {
                                    kvs.deleteRow("pt-crawl-queue", key);
                                    kvs.put("pt-crawl-visited", key, "value", url);
                                } else if (!throttled) {
                                    // For unexpected errors, drop the URL so the crawl can continue.
                                    kvs.deleteRow("pt-crawl-queue", key);
                                    kvs.put("pt-crawl-visited", key, "value", url);
                                }
                            } catch (Exception ignore) {
                            }
                        }
                    } catch (Exception e) {
                        // Drop the URL if we encountered an unexpected failure outside the main flow.
                        try {
                            KVSClient kvs = new KVSClient(kvsCoord);
                            kvs.deleteRow("pt-crawl-queue", Hasher.hash(url));
                            kvs.put("pt-crawl-visited", Hasher.hash(url), "value", url);
                        } catch (Exception ignored) {
                        }
                        return java.util.Collections.<String>emptyList();
                    }

                });

            } catch (Exception e) {
                break;
            }
        }

        context.output("✅ Crawler job completed.\n");
        System.out.flush();
    }

    private static List<String> extractUrls(String bodyString) {
        List<String> newUrls = new ArrayList<>();
        if (bodyString == null || bodyString.isEmpty()) {
            return newUrls;
        }
        String bodyLowercase = bodyString.toLowerCase(Locale.ROOT);
        int i = 0;
        while ((i = bodyLowercase.indexOf("<a", i)) != -1) {
            int tagEnd = bodyLowercase.indexOf(">", i);
            if (tagEnd == -1) break;

            int hrefPosInLower = bodyLowercase.indexOf("href=", i);
            if (hrefPosInLower == -1 || hrefPosInLower > tagEnd) {
                i = tagEnd + 1;
                continue;
            }

            int valStart = hrefPosInLower + 5;
            if (valStart >= bodyString.length()) {
                i = tagEnd + 1;
                continue;
            }

            char first = bodyString.charAt(valStart);
            int urlStart, urlEnd;

            if (first == '"' || first == '\'') {
                urlStart = valStart + 1;
                urlEnd = bodyString.indexOf(first, urlStart);
                if (urlEnd == -1 || urlEnd > tagEnd) {
                    i = tagEnd + 1;
                    continue;
                }
            } else {
                urlStart = valStart;
                int sp = bodyString.indexOf(' ', urlStart);
                int gt = bodyString.indexOf('>', urlStart);
                urlEnd = (sp == -1) ? gt : (gt == -1 ? sp : Math.min(sp, gt));
                if (urlEnd == -1 || urlEnd > tagEnd) urlEnd = tagEnd;
            }

            if (urlStart >= 0 && urlEnd > urlStart) {
                String u = bodyString.substring(urlStart, urlEnd).trim();
                if (!u.isEmpty()) newUrls.add(u);
            }

            i = tagEnd + 1;
        }
        return newUrls;
    }

    private static String normalizeAndFilterUrl(String baseUrl, String url) {
       if (url == null) {
            return null;
        }
        url = url.trim();
        if (url.isEmpty()) {
            return null;
        }

        // Drop fragment
        int hash = url.indexOf('#');
        if (hash >= 0) {
            url = url.substring(0, hash);
        }
        if (url.isEmpty()) {
            return null;
        }

        // Drop obvious non-HTTP stuff
        String lower = url.toLowerCase(Locale.ROOT);
        if (lower.startsWith("mailto:") || lower.startsWith("javascript:")) {
            return null;
        }

        // Parse base URL
        String[] base = URLParser.parseURL(baseUrl);
        String baseProtocol = base[0] != null ? base[0].toLowerCase(Locale.ROOT) : "http";
        String baseHost     = base[1] != null ? base[1].toLowerCase(Locale.ROOT) : "";
        String basePort     = (base[2] != null && !base[2].isEmpty())
                ? base[2]
                : (baseProtocol.equals("https") ? "443" : "80");
        String basePath     = base[3] != null ? base[3] : "/";

        String proto = baseProtocol;
        String host  = baseHost;
        String port  = basePort;
        String path;
        
        try {
            // Fully absolute URL
            if (url.startsWith("http://") || url.startsWith("https://")) {
                String[] parts = URLParser.parseURL(url);
                proto = (parts[0] != null) ? parts[0].toLowerCase(Locale.ROOT) : "http";
                host  = (parts[1] != null) ? parts[1].toLowerCase(Locale.ROOT) : "";
                port  = (parts[2] != null && !parts[2].isEmpty())
                        ? parts[2]
                        : (proto.equals("https") ? "443" : "80");
                path  = (parts[3] != null && !parts[3].isEmpty()) ? parts[3] : "/";

                // Protocol-relative URL
            } else if (url.startsWith("//")) {
                String withoutSlashes = url.substring(2);
                int slash = withoutSlashes.indexOf('/');
                if (slash >= 0) {
                    host = withoutSlashes.substring(0, slash).toLowerCase(Locale.ROOT);
                    path = withoutSlashes.substring(slash);
                } else {
                    host = withoutSlashes.toLowerCase(Locale.ROOT);
                    path = "/";
                }
                proto = baseProtocol;
                port  = (proto.equals("https") ? "443" : "80");

                // Absolute-path URL on same host
            } else if (url.startsWith("/")) {
                proto = baseProtocol;
                host  = baseHost;
                port  = basePort;
                path  = url;

                // Relative path
            } else {
                proto = baseProtocol;
                host  = baseHost;
                port  = basePort;
                String baseDir;
                int lastSlash = basePath.lastIndexOf('/');
                if (lastSlash >= 0) {
                    baseDir = basePath.substring(0, lastSlash + 1);
                } else {
                    baseDir = "/";
                }
                path = baseDir + url;
            }

            if (host == null || host.isEmpty()) {
                return null;
            }

            if (!path.startsWith("/")) {
                path = "/" + path;
            }

            // Normalize /./ and /../ segments
            while (path.contains("/./")) {
                path = path.replace("/./", "/");
            }
            // Collapse simple /segment/../ patterns repeatedly
            while (path.contains("/../")) {
                path = path.replaceAll("/[^/]+/\\.\\./", "/");
            }
            if (path.isEmpty()) {
                path = "/";
            }

            // Only keep http/https
            if (!"http".equals(proto) && !"https".equals(proto)) {
                return null;
            }

            // Default ports
            if (port == null || port.isEmpty()) {
                port = proto.equals("https") ? "443" : "80";
            }

        } catch (Exception e) {
            // Malformed
            return null;
        }

        // Omit default ports for cleaner URLs
        boolean isDefaultPort =
                ("https".equals(proto) && "443".equals(port)) ||
                        ("http".equals(proto) && "80".equals(port));

        if (isDefaultPort) {
            return proto + "://" + host + path;
        } else {
            return proto + "://" + host + ":" + port + path;
        }
    }


    private static String normalizeSeedUrl(String url) {
        if (url == null) return null;
        int i = url.indexOf('#');
        if (i >= 0) url = url.substring(0, i);
        String[] p = URLParser.parseURL(url);
        String proto = (p[0] != null) ? p[0] : "http";
        String host  = (p[1] != null) ? p[1] : "";
        if (host.isEmpty()) return null;
        String port  = (p[2] != null && !p[2].isEmpty())
                ? p[2]
                : (proto.equals("https") ? "443" : "80");
        String path  = (p[3] != null && !p[3].isEmpty()) ? p[3] : "/";
        if (!path.startsWith("/")) path = "/" + path;
        
        // Omit default ports for cleaner URLs
        boolean isDefaultPort = (proto.equals("https") && port.equals("443")) || 
                               (proto.equals("http") && port.equals("80"));
        
        if (isDefaultPort) {
            return proto + "://" + host + path;
        } else {
            return proto + "://" + host + ":" + port + path;
        }
    }

    private static String extractHostKey(String canonical) {
        String[] parts = URLParser.parseURL(canonical);
        String proto = (parts[0] != null) ? parts[0].toLowerCase(Locale.ROOT) : "http";
        String host  = (parts[1] != null) ? parts[1].toLowerCase(Locale.ROOT) : "";
        String port  = (parts[2] != null && !parts[2].isEmpty())
                ? parts[2]
                : (proto.equals("https") ? "443" : "80");
        return host.isEmpty() ? "" : host + ":" + port;
    }

    private static String extractProtocol(String canonical) {
        String[] parts = URLParser.parseURL(canonical);
        String proto = (parts[0] != null) ? parts[0].toLowerCase(Locale.ROOT) : "http";
        return proto.isEmpty() ? "" : proto;
    }

    private static String extractPath(String canonical) {
        String[] parts = URLParser.parseURL(canonical);
        String path = (parts[3] != null && !parts[3].isEmpty()) ? parts[3] : "/";
        if (!path.startsWith("/")) path = "/" + path;
        return path;
    }

    private static void updateLastAccess(KVSClient kvs, String host, long now) throws IOException {
        kvs.put("hosts", host, "lastAccess", Long.toString(now));
    }

    private static boolean isRecent(KVSClient kvs, String host, long now) throws IOException {
        Row row = kvs.getRow("hosts", host);
        long last = 0L;
        if (row != null) {
            String lastStr = row.get("lastAccess");
            if (lastStr != null) {
                try { last = Long.parseLong(lastStr); } catch (NumberFormatException ignored) {}
            }
        }
        double delaySecs = getRobotsCrawlDelay(kvs, host);
        long delayMs = (long) Math.ceil(delaySecs * 1000.0);

        return (now - last) < delayMs;
    }

    private static double getRobotsCrawlDelay(KVSClient kvs, String host) throws IOException {
        Row row = kvs.getRow("hosts", host);
        if (row == null) return DEFAULT_DELAY;
        String cd = row.get("crawlDelay");
        if (cd == null) return DEFAULT_DELAY;
        try {
            return Double.parseDouble(cd);
        } catch (NumberFormatException ignored) {
            return DEFAULT_DELAY;
        }
    }

    private static boolean ensureRobotsFetched(KVSClient kvs, String protocol, String host) throws IOException {
        Row row = kvs.getRow("hosts", host);
        if (row != null && row.get("robotsFetched") != null) {
            return true;
        }
        String robotsURL = protocol + "://" + host + "/robots.txt";
        int status = -1;
        String body = "";
        long now = System.currentTimeMillis();
        HttpURLConnection c = null;
        try {
            c = openWithHttpsFallback(robotsURL);
            c.setRequestMethod("GET");
            c.setRequestProperty("User-Agent", UA);
            c.setConnectTimeout(10000);
            c.setReadTimeout(10000);
            c.setInstanceFollowRedirects(true);
            status = c.getResponseCode();

            InputStream in = null;
            if (status >= 200 && status < 300) {
                in = c.getInputStream();
            } else {
                in = c.getErrorStream();
            }

            if (in != null) {
                body = readUpTo(in, MAX_ROBOTS_BYTES);
            } else {
                body = "";
            }
        } catch (Exception ignored) {
        } finally {
            if (c != null) {
                try {
                    c.disconnect();
                } catch (Exception ignore) {
                }
            }
        }

        Row hostRow = new Row(host);
        hostRow.put("robotsFetched", "1");
        hostRow.put("robotsStatus", Integer.toString(status));
        hostRow.put("robotsTxt", body == null ? "" : body);
        hostRow.put("robotsFetchedAt", Long.toString(now));

        if (status >= 200 && status < 300 && body != null && !body.isEmpty()) {
            String cd = extractCrawlDelay(body, UA);
            if (cd != null) {
                hostRow.put("crawlDelay", cd);
            }
        }
        kvs.putRow("hosts", hostRow);
        return true;
    }

    private static String readUpTo(InputStream in, int maxBytes) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int remaining = maxBytes;

        while (remaining > 0) {
            int read = in.read(buf, 0, Math.min(buf.length, remaining));
            if (read == -1) break;
            out.write(buf, 0, read);
            remaining -= read;
        }

        return out.toString(StandardCharsets.UTF_8);
    }


    private static boolean robotsAllows(KVSClient kvs, String host, String path) throws IOException {
        Row row = kvs.getRow("hosts", host);
        if (row == null) {
            return true;
        }

        int status = 0;
        String statusStr = row.get("robotsStatus");
        if (statusStr != null) {
            try {
                status = Integer.parseInt(statusStr);
            } catch (NumberFormatException ignore) {
                status = 0;
            }
        }
        if (status == 404 || status <= 0) {
            return true;
        }
        if (status < 200 || status >= 300) {
            return true;
        }
        String robots = row.get("robotsTxt");
        if (robots == null || robots.isEmpty()) {
            return true;
        }
        if (!path.startsWith("/")) {
            int idx = path.indexOf('/', path.indexOf("://") + 3);
            if (idx >= 0) {
                path = path.substring(idx);
            } else {
                path = "/";
            }
        }

        List<String> rulesForUA = new ArrayList<>();
        List<String> rulesForStar = new ArrayList<>();
        parseRobotsRulesOrdered(robots, UA, rulesForUA, rulesForStar);

        List<String> rules = rulesForUA.isEmpty() ? rulesForStar : rulesForUA;
        if (rules.isEmpty()) {
            return true;
        }

        boolean isAllow = true;
        String longestMatch = null;

        for (String rule : rules) {
            if (rule == null || rule.length() < 3) {
                continue;
            }

            char kind = rule.charAt(0);
            if (rule.charAt(1) != ' ') {
                continue;
            }

            String prefix = rule.substring(2);
            if (prefix.isEmpty()) {
                continue;
            }

            if (path.startsWith(prefix)) {
                if (longestMatch == null || prefix.length() > longestMatch.length()) {
                    longestMatch = prefix;
                    isAllow = (kind == 'A');
                }
            }
        }
        return longestMatch == null || isAllow;
    }



    private static void parseRobotsRulesOrdered(String robots, String ua, List<String> rulesForUA, List<String> rulesForStar) {
        if (robots == null) return;
        String[] lines = robots.replace("\r", "").split("\n");

        int group = 0;
        for (String raw : lines) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            int colon = line.indexOf(':');
            if (colon <= 0) continue;

            String key = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            String val = line.substring(colon + 1).trim();

            switch (key) {
                case "user-agent": {
                    String vLower = val.toLowerCase(Locale.ROOT);
                    if (vLower.equals(ua.toLowerCase(Locale.ROOT))) {
                        group = 1;
                    } else if (vLower.equals("*")) {
                        group = 2;
                    } else {
                        group = 3;
                    }
                    break;
                }
                case "allow": {
                    if (group == 1) rulesForUA.add("A:" + val);
                    else if (group == 2) rulesForStar.add("A:" + val);
                    break;
                }
                case "disallow": {
                    if (group == 1) rulesForUA.add("D:" + val);
                    else if (group == 2) rulesForStar.add("D:" + val);
                    break;
                }
                case "crawl-delay": {
                    break;
                }
                default:
            }
        }
    }

    private static String extractCrawlDelay(String robots, String ua) {
        if (robots == null) return null;
        String[] lines = robots.replace("\r", "").split("\n");

        int group = 0;
        String firstTarget = null;
        String firstStar = null;

        for (String raw : lines) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            int colon = line.indexOf(':');
            if (colon <= 0) continue;

            String key = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            String val = line.substring(colon + 1).trim();

            if ("user-agent".equals(key)) {
                String vLower = val.toLowerCase(Locale.ROOT);
                if (vLower.equals(ua.toLowerCase(Locale.ROOT))) group = 1;
                else if (vLower.equals("*")) group = 2;
                else group = 3;
                continue;
            }

            if ("crawl-delay".equals(key)) {
                if (group == 1 && firstTarget == null) firstTarget = val;
                else if (group == 2 && firstStar == null) firstStar = val;
            }
        }

        String chosen = (firstTarget != null) ? firstTarget : firstStar;
        if (chosen == null) return null;
        try {
            Double.parseDouble(chosen);
            return chosen;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static boolean shouldCrawl(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        String[] parts = URLParser.parseURL(url);
        String protocol = (parts[0] != null) ? parts[0].toLowerCase(Locale.ROOT) : "http";
        String host     = (parts[1] != null) ? parts[1].toLowerCase(Locale.ROOT) : "";
        String path     = (parts[3] != null && !parts[3].isEmpty()) ? parts[3] : "/";

        if (!protocol.equals("http") && !protocol.equals("https")) {
            return false;
        }
        if (host.isEmpty()) {
            return false;
        }

        String lowerPath = path.toLowerCase(Locale.ROOT);
        String lowerUrl  = url.toLowerCase(Locale.ROOT);

        //Host blacklist
        if (BLACKLISTED_HOSTS.contains(host)) {
            return false;
        }
        //Extension blacklist
        for (String ext : BLACKLISTED_EXTENSIONS) {
            if (lowerPath.endsWith(ext)) {
                return false;
            }
        }
        //Substring/path blacklist
        for (String s : BLACKLISTED_SUBSTRINGS) {
            if (lowerUrl.contains(s)) {
                return false;
            }
        }
        //Language blacklist
        for (String s : BLACKLISTED_LANGUAGE) {
            if (lowerPath.contains(s)) {
                return false;
            }
        }
        return true;
    }

    private static void saveToTable(KVSClient kvs, String key, String url, Integer code, String ctype, byte[] body, String status, boolean done) throws IOException {
        Row row = new Row(key);
        row.put("url", url);
        if (code != null) {
            row.put("responseCode", String.valueOf(code));
        }

        if (ctype != null) {
            row.put("contentType", ctype);
        }

        if (status != null) {
            row.put("status", status);
        }

        row.put("done", done ? "1" : "0");
        if (body != null) {
            row.put("page", body);
            row.put("length", String.valueOf(body.length));
        }

        kvs.putRow("pt-crawl", row);

        /*
        kvs.put("pt-crawl", key, "url", url);
        if (code != null) kvs.put("pt-crawl", key, "responseCode", String.valueOf(code));
        if (ctype != null) kvs.put("pt-crawl", key, "contentType", ctype);
        if (status != null) kvs.put("pt-crawl", key, "status", status);
        kvs.put("pt-crawl", key, "done", done ? "1" : "0");
        if (body != null) {
            kvs.put("pt-crawl", key, "page", body);
            kvs.put("pt-crawl", key, "length", String.valueOf(body.length));
        }

         */
    }
    
    private static boolean reachedTargetCrawl(KVSClient kvs, int target) throws IOException {
        return kvs.count("pt-crawl") >= target;
    }  


    private static String hostname(String url) {
        if (url == null || url.isBlank()) return "";
        try {
            URI uri = URI.create(url.trim());
            String host = uri.getHost();
            if (host == null) {
                // handle schemeless like //example.com/path
                uri = URI.create((url.startsWith("//") ? "http:" : "http://") + url.trim());
                host = uri.getHost();
            }
            return host != null ? host.toLowerCase(Locale.ROOT) : "";
        } catch (IllegalArgumentException e) {
            return "";
        }
    }
    
    
    private static boolean underHostCap(String url) {
        String h = hostname(url);
        if (h.isEmpty()) return false;
        return hostCounts.getOrDefault(h, 0) < MAX_PAGES_PER_HOST;
    }

    private static HttpURLConnection openWithHttpsFallback(String urlStr) throws Exception {
        URI uri = URI.create(urlStr.trim());
        URL url = uri.toURL();
    
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setInstanceFollowRedirects(true);
            return conn;
        } catch (SSLHandshakeException e) {
            if ("https".equalsIgnoreCase(uri.getScheme())) {
                int port = (uri.getPort() == -1) ? 80 : uri.getPort();
                // build equivalent http:// URI
                URI httpUri = new URI(
                        "http",
                        uri.getUserInfo(),
                        uri.getHost(),
                        port,
                        uri.getPath(),
                        uri.getQuery(),
                        uri.getFragment());
                URL httpUrl = httpUri.toURL();
                HttpURLConnection httpConn = (HttpURLConnection) httpUrl.openConnection();
                httpConn.setInstanceFollowRedirects(true);
                return httpConn;
            }
            throw e;
        }
    }

    //HREF filtering helpers
    private static boolean isSkippableHref(String href) {
        if (href == null) return true;
        String h = href.trim();
        if (h.isEmpty() || h.startsWith("#")) return true;
        String lower = h.toLowerCase();
        return lower.startsWith("mailto:")
            || lower.startsWith("tel:")
            || lower.startsWith("javascript:")
            || lower.startsWith("data:")
            || lower.startsWith("about:")
            || lower.startsWith("ftp:");
    }

    private static String unwrapUrlDefense(String url) {
        if (url == null) return null;
        String u = url;
        String lower = u.toLowerCase();
        if (!lower.contains("urldefense.com")) {
            return u;
        }
        try {
            int a = u.indexOf("__");
            if (a < 0) return null;
            int b = u.indexOf("__", a + 2);
            if (b <= a + 2) return null;
            String inner = u.substring(a + 2, b);
            return java.net.URLDecoder.decode(inner, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return null;
        }
    }

    private static HttpURLConnection openConnectionAndConfigure(
        String url,
        String method,
        boolean followRedirects,
        int connectTimeoutMs,
        int readTimeoutMs,
        boolean identityEncoding) throws Exception {

    HttpURLConnection conn = openWithHttpsFallback(url);
    conn.setInstanceFollowRedirects(followRedirects);
    conn.setRequestMethod(method);
    conn.setRequestProperty("User-Agent", UA);
    if (identityEncoding) {
        conn.setRequestProperty("Accept-Encoding", "identity");
        conn.setRequestProperty("Connection", "close");
    }
    conn.setConnectTimeout(connectTimeoutMs);
    conn.setReadTimeout(readTimeoutMs);
    return conn;
}


}