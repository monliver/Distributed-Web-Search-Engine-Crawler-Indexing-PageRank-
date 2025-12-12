package jobs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import flame.FlameContext;
import flame.FlamePair;
import flame.FlamePairRDD;
import flame.FlameRDD;
import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;

/** Streaming PageRank with staged resume support. */
public class PageRank {

    private static final double DAMPING_FACTOR = 0.85;
    private static final double CONVERGENCE_THRESHOLD = 0.001;
    private static final int MAX_ITERATIONS = 20;
    private static final int MAX_OUT_LINKS = 30;
    private static final int MAX_HTML_BYTES = 20_000; // ~20KB per page

    private static final String TABLE_GRAPH = "pt-pagerank-graph";
    private static final String TABLE_RANKS_CUR = "pt-pagerank-ranks-old";
    private static final String TABLE_RANKS_NEXT = "pt-pagerank-ranks-new";
    private static final String TABLE_CONTRIB = "pt-pagerank-contrib";
    private static final String TABLE_PAGERANKS = "pt-pageranks";
    private static final String TABLE_PROGRESS = "pt-pagerank-progress";

    private static final String PROGRESS_ROW_META = "__meta__";
    private static final String PROGRESS_COL_STAGE = "stage";
    private static final String PROGRESS_COL_TOTAL = "total";
    private static final String PROGRESS_COL_ITERATION = "iteration";
    private static final String PROGRESS_COL_RESIDUAL = "residual";
    private static final String PROGRESS_COL_RANKS = "ranks";

    private static final String PROGRESS_STAGE_GRAPH = "graph-built";
    private static final String PROGRESS_RANKS_INITIALIZED = "initialized";

    private static final String PROGRESS_DONE_COL = "done";

    private static final String SENTINEL_DANGLING = "__dangling__";

    public static void run(FlameContext context, String[] args) throws Exception {
        context.output("========================================");
        context.output("Streaming PageRank");
        context.output("========================================");

        KVSClient kvs = context.getKVS();

        String stage = readProgress(kvs, PROGRESS_COL_STAGE);
        boolean graphReady = PROGRESS_STAGE_GRAPH.equals(stage);
        boolean graphBuilding = "building".equals(stage);

        if (!graphReady) {
            if (graphBuilding) {
                context.output("[Phase 1] Resuming metadata + adjacency build...");
            } else {
                context.output("[Phase 1] Building metadata + adjacency (fresh run)...");
            }
            buildGraph(context, kvs, graphBuilding);
        } else {
            context.output("[Phase 1] Resume detected – keeping existing graph tables.");
        }

        int totalPages = resolveTotalPages(kvs);
        if (totalPages <= 0) {
            context.output("No valid pages to rank (totalPages=0).");
            context.output("OK");
            return;
        }

        if (!PROGRESS_RANKS_INITIALIZED.equals(readProgress(kvs, PROGRESS_COL_RANKS))) {
            context.output("[Phase 2] Seeding initial ranks...");
            seedInitialRanks(context, kvs, totalPages);
        } else {
            context.output("[Phase 2] Resume detected – ranks already initialized.");
        }

        int completedIterations = parseIntSafe(readProgress(kvs, PROGRESS_COL_ITERATION), 0);
        context.output("[Phase 3] Starting iterations from iteration " + (completedIterations + 1));

        double residual = Double.MAX_VALUE;
        for (int iteration = completedIterations + 1; iteration <= MAX_ITERATIONS; iteration++) {
            residual = runIteration(context, kvs, totalPages, iteration);
            storeProgress(kvs, PROGRESS_COL_ITERATION, Integer.toString(iteration));
            storeProgress(kvs, PROGRESS_COL_RESIDUAL, Double.toString(residual));
            context.output(String.format("  Iteration %d complete (residual=%.6f)", iteration, residual));
            if (residual < CONVERGENCE_THRESHOLD) {
                context.output(String.format("  ✓ Converged after %d iteration(s)", iteration));
                break;
            }
        }

        context.output("[Phase 4] Writing final PageRank scores back to pt-pageranks...");
        writeFinalPageranks(kvs);

        context.output("========================================");
        context.output("PageRank job completed (residual=" + residual + ")");
        context.output("========================================");
        context.output("OK");
    }

    // ---------------------------------------------------------------------
    // Phase 1: Build metadata + adjacency
    // ---------------------------------------------------------------------

    private static void buildGraph(FlameContext context, KVSClient kvs, boolean resume) throws Exception {
        if (!resume) {
            clearTables(kvs);
        }
        storeProgress(kvs, PROGRESS_COL_STAGE, "building");
        storeProgress(kvs, PROGRESS_COL_ITERATION, "0");
        storeProgress(kvs, PROGRESS_COL_RESIDUAL, Double.toString(Double.POSITIVE_INFINITY));

        FlameRDD crawlData = context.fromTable("pt-crawl", rowObj -> {
            Row row = (Row) rowObj;
            String url = row.get("url");
            byte[] pageBytes = row.getBytes("page");
            if (url == null || pageBytes == null || pageBytes.length < 100) {
                return null;
            }
            int len = Math.min(pageBytes.length, MAX_HTML_BYTES);
            String html = new String(pageBytes, 0, len, StandardCharsets.UTF_8);
            return url + "\t" + html;
        }).filter(s -> s != null);

        crawlData.forEach(new CrawlProcessor(kvs.getCoordinator()));

        int totalPages = kvs.count(TABLE_GRAPH);
        storeProgress(kvs, PROGRESS_COL_TOTAL, Integer.toString(totalPages));
        storeProgress(kvs, PROGRESS_COL_STAGE, PROGRESS_STAGE_GRAPH);
        storeProgress(kvs, PROGRESS_COL_RANKS, "pending");
        context.output("  ✓ Graph built. Total pages=" + totalPages);
    }

    private static class CrawlProcessor implements FlameRDD.StringToVoid {
        private final String kvsCoordinator;
        private transient KVSClient workerKvs;

        CrawlProcessor(String kvsCoordinator) {
            this.kvsCoordinator = kvsCoordinator;
        }

        @Override
        public void op(String record) throws Exception {
            if (record == null || record.isEmpty()) {
                return;
            }

            int tab = record.indexOf('\t');
            if (tab <= 0) {
                return;
            }

            String url = record.substring(0, tab);
            String html = record.substring(tab + 1);
            if (url.isEmpty() || html.isEmpty()) {
                return;
            }

            if (workerKvs == null) {
                workerKvs = new KVSClient(kvsCoordinator);
            }

            String hash = Hasher.hash(url);
            if (alreadyProcessed(workerKvs, hash)) {
                return;
            }

            String title = extractTitle(html);
            if (title.isEmpty()) {
                title = url;
            }
            String snippet = generateSnippet(html);

            // Metadata row (shared with frontend)
            workerKvs.put(TABLE_PAGERANKS, hash, "url", url);
            workerKvs.put(TABLE_PAGERANKS, hash, "title", title);
            workerKvs.put(TABLE_PAGERANKS, hash, "snippet", snippet);

            // Adjacency list
            workerKvs.put(TABLE_GRAPH, hash, "url", url);
            Set<String> outLinks = extractLinks(url, html);
            int added = 0;
            for (String dest : outLinks) {
                if (added >= MAX_OUT_LINKS) {
                    break;
                }
                String destHash = Hasher.hash(dest);
                try {
                    workerKvs.appendCapped(
                            TABLE_GRAPH,
                            hash,
                            "edge",
                            destHash,
                            MAX_OUT_LINKS,
                            ',',
                            Math.max(4, MAX_OUT_LINKS),
                            MAX_OUT_LINKS);
                    added++;
                } catch (Exception e) {
                    // Swallow and continue so a single bad edge does not abort the job
                }
            }

            workerKvs.put(TABLE_PROGRESS, hash, PROGRESS_DONE_COL, "1");
        }

        private boolean alreadyProcessed(KVSClient kvs, String hash) {
            try {
                byte[] done = kvs.get(TABLE_PROGRESS, hash, PROGRESS_DONE_COL);
                return done != null;
            } catch (Exception ignored) {
                return false;
            }
        }
    }

    // ---------------------------------------------------------------------
    // Phase 2: Seed initial ranks
    // ---------------------------------------------------------------------

    private static void seedInitialRanks(FlameContext context, KVSClient kvs, int totalPages) throws Exception {
        kvs.delete(TABLE_RANKS_CUR);
        kvs.delete(TABLE_RANKS_NEXT);

        double baseRank = (totalPages > 0) ? (1.0 / totalPages) : 0.0;
        String baseString = Double.toString(baseRank);

        FlameRDD vertexIds = context.fromTable(TABLE_GRAPH, rowObj -> {
            Row row = (Row) rowObj;
            if (row.get("url") == null) {
                return null;
            }
            return row.key();
        }).filter(s -> s != null);

        vertexIds.forEach(new InitialRankWriter(kvs.getCoordinator(), baseString));

        storeProgress(kvs, PROGRESS_COL_RANKS, PROGRESS_RANKS_INITIALIZED);
        storeProgress(kvs, PROGRESS_COL_ITERATION, "0");
        storeProgress(kvs, PROGRESS_COL_RESIDUAL, Double.toString(Double.POSITIVE_INFINITY));
        kvs.delete(TABLE_CONTRIB);
    }

    private static class InitialRankWriter implements FlameRDD.StringToVoid {
        private final String coordinator;
        private final String rank;
        private transient KVSClient workerKvs;

        InitialRankWriter(String coordinator, String rank) {
            this.coordinator = coordinator;
            this.rank = rank;
        }

        @Override
        public void op(String key) throws Exception {
            if (key == null) {
                return;
            }
            if (workerKvs == null) {
                workerKvs = new KVSClient(coordinator);
            }
            workerKvs.put(TABLE_RANKS_CUR, key, "rank", rank);
        }
    }

    // ---------------------------------------------------------------------
    // Phase 3: Iterative PageRank
    // ---------------------------------------------------------------------

    private static double runIteration(FlameContext context, KVSClient kvs, int totalPages, int iteration) throws Exception {
        kvs.delete(TABLE_CONTRIB);
        kvs.delete(TABLE_RANKS_NEXT);

        FlameRDD graphRows = context.fromTable(TABLE_GRAPH, rowObj -> encodeGraphRow((Row) rowObj)).filter(s -> s != null);

        FlamePairRDD contributions = graphRows.flatMapToPair(new ContributionMapper(kvs.getCoordinator()));

        FlamePairRDD aggregated = contributions.foldByKey("0.0", (acc, value) -> {
            double a = parseDoubleSafe(acc, 0.0);
            double b = parseDoubleSafe(value, 0.0);
            return Double.toString(a + b);
        });

        aggregated.saveAsTable(TABLE_CONTRIB);

        double danglingSum = parseDoubleSafe(readTableValue(kvs, TABLE_CONTRIB, SENTINEL_DANGLING), 0.0);
        if (danglingSum != 0.0) {
            try { kvs.deleteRow(TABLE_CONTRIB, SENTINEL_DANGLING); } catch (Exception ignored) {}
        }

        double danglingShare = (totalPages > 0) ? (DAMPING_FACTOR * danglingSum / totalPages) : 0.0;
        double base = (totalPages > 0) ? ((1.0 - DAMPING_FACTOR) / totalPages) : 0.0;

        double maxDiff = 0.0;
        Iterator<Row> iter = kvs.scan(TABLE_GRAPH);
        while (iter.hasNext()) {
            Row row = iter.next();
            String key = row.key();
            double inbound = parseDoubleSafe(readTableValue(kvs, TABLE_CONTRIB, key), 0.0);
            double newRank = base + (DAMPING_FACTOR * inbound) + danglingShare;
            double oldRank = parseDoubleSafe(readTableColumn(kvs, TABLE_RANKS_CUR, key, "rank"), 0.0);
            double diff = Math.abs(newRank - oldRank);
            if (diff > maxDiff) {
                maxDiff = diff;
            }
            kvs.put(TABLE_RANKS_NEXT, key, "rank", Double.toString(newRank));
        }

        kvs.delete(TABLE_RANKS_CUR);
        kvs.rename(TABLE_RANKS_NEXT, TABLE_RANKS_CUR);
        kvs.delete(TABLE_CONTRIB);

        context.output(String.format("  Iteration %d residual: %.6f", iteration, maxDiff));
        return maxDiff;
    }

    private static class ContributionMapper implements FlameRDD.StringToPairIterable {
        private final String coordinator;
        private transient KVSClient workerKvs;

        ContributionMapper(String coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public Iterable<FlamePair> op(String record) throws Exception {
            List<FlamePair> output = new ArrayList<>();
            if (record == null || record.isEmpty()) {
                return output;
            }
            if (workerKvs == null) {
                workerKvs = new KVSClient(coordinator);
            }

            String[] parts = record.split("\t");
            if (parts.length == 0) {
                return output;
            }

            String srcHash = parts[0];
            double rank = parseDoubleSafe(readTableColumn(workerKvs, TABLE_RANKS_CUR, srcHash, "rank"), 0.0);

            Set<String> destinations = new HashSet<>();
            for (int i = 1; i < parts.length; i++) {
                String chunk = parts[i];
                if (chunk == null || chunk.isEmpty()) {
                    continue;
                }
                String[] edges = chunk.split(",");
                for (String edge : edges) {
                    if (!edge.isEmpty()) {
                        destinations.add(edge);
                    }
                }
            }

            if (destinations.isEmpty()) {
                if (rank != 0.0) {
                    output.add(new FlamePair(SENTINEL_DANGLING, Double.toString(rank)));
                }
                return output;
            }

            double share = rank / destinations.size();
            String shareStr = Double.toString(share);
            for (String dest : destinations) {
                output.add(new FlamePair(dest, shareStr));
            }
            return output;
        }
    }

    private static String encodeGraphRow(Row row) {
        if (row == null || row.get("url") == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(row.key());
        for (String column : row.columns()) {
            if (column.startsWith("edge")) {
                String val = row.get(column);
                if (val != null && !val.isEmpty()) {
                    sb.append('\t').append(val);
                }
            }
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------
    // Phase 4: Write final pageranks
    // ---------------------------------------------------------------------

    private static void writeFinalPageranks(KVSClient kvs) throws Exception {
        Iterator<Row> iter = kvs.scan(TABLE_RANKS_CUR);
        while (iter.hasNext()) {
            Row row = iter.next();
            String key = row.key();
            String rank = row.get("rank");
            if (rank != null) {
                kvs.put(TABLE_PAGERANKS, key, "pagerank", rank);
            }
        }
    }

    // ---------------------------------------------------------------------
    // Progress helpers
    // ---------------------------------------------------------------------

    private static String readProgress(KVSClient kvs, String column) {
        try {
            return readTableColumn(kvs, TABLE_PROGRESS, PROGRESS_ROW_META, column);
        } catch (Exception e) {
            return null;
        }
    }

    private static void storeProgress(KVSClient kvs, String column, String value) {
        try {
            kvs.put(TABLE_PROGRESS, PROGRESS_ROW_META, column, value);
        } catch (Exception ignored) {}
    }

    private static int resolveTotalPages(KVSClient kvs) throws Exception {
        int fromProgress = parseIntSafe(readProgress(kvs, PROGRESS_COL_TOTAL), 0);
        if (fromProgress > 0) {
            return fromProgress;
        }
        int counted = kvs.count(TABLE_GRAPH);
        if (counted > 0) {
            storeProgress(kvs, PROGRESS_COL_TOTAL, Integer.toString(counted));
        }
        return counted;
    }

    private static void clearTables(KVSClient kvs) {
        try { kvs.delete(TABLE_GRAPH); } catch (Exception ignored) {}
        try { kvs.delete(TABLE_RANKS_CUR); } catch (Exception ignored) {}
        try { kvs.delete(TABLE_RANKS_NEXT); } catch (Exception ignored) {}
        try { kvs.delete(TABLE_CONTRIB); } catch (Exception ignored) {}
        try { kvs.delete(TABLE_PAGERANKS); } catch (Exception ignored) {}
        try { kvs.delete(TABLE_PROGRESS); } catch (Exception ignored) {}
    }

    // ---------------------------------------------------------------------
    // Utility helpers
    // ---------------------------------------------------------------------

    private static String readTableColumn(KVSClient kvs, String table, String row, String column) {
        try {
            byte[] data = kvs.get(table, row, column);
            return data == null ? null : new String(data);
        } catch (Exception e) {
            return null;
        }
    }

    private static String readTableValue(KVSClient kvs, String table, String row) {
        try {
            Row r = kvs.getRow(table, row);
            if (r == null) {
                return null;
            }
            return r.get("value");
        } catch (Exception e) {
            return null;
        }
    }

    private static int parseIntSafe(String value, int fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static double parseDoubleSafe(String value, double fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    // ======================================================================
    // Helper methods preserved from previous implementation
    // ======================================================================

    /** Extract <title>...</title> text from HTML. */
    private static String extractTitle(String html) {
        if (html == null) return "";
        try {
            String lower = html.toLowerCase(Locale.ROOT);
            int start = lower.indexOf("<title");
            if (start == -1) return "";
            start = html.indexOf(">", start);
            if (start == -1) return "";
            start += 1;
            int end = lower.indexOf("</title>", start);
            if (end == -1 || end <= start) return "";
            String title = html.substring(start, end).trim();
            title = cleanText(title);
            if (title.length() > 120) {
                title = title.substring(0, 120) + "...";
            }
            return title;
        } catch (Exception e) {
            return "";
        }
    }

    /** Generate a short snippet from body text. */
    private static String generateSnippet(String html) {
        // Priority 1: Try meta description tags
        String metaDescription = extractMetaDescription(html);
        if (metaDescription != null && !metaDescription.isEmpty()) {
            if (metaDescription.length() > 200) {
                return metaDescription.substring(0, 200) + "...";
            }
            return metaDescription;
        }

        // Priority 2: Try semantic tags
        String semanticContent = extractSemanticContent(html);
        String plain;

        if (semanticContent != null) {
            plain = cleanText(semanticContent);
        } else {
            // Priority 3: Skip first 300 chars to avoid navigation menus
            String body = extractBodyText(html);
            if (body.length() > 300) {
                body = body.substring(300);
            }
            plain = cleanText(body);
        }

        if (plain.length() > 200) {
            return plain.substring(0, 200) + "...";
        }
        return plain;
    }

    /** Extract description from meta tags (og:description, description, twitter:description). */
    private static String extractMetaDescription(String html) {
        if (html == null) return null;
        String lower = html.toLowerCase(Locale.ROOT);

        // Try Open Graph description first (og:description)
        String ogDesc = extractMetaTag(html, lower, "property", "og:description");
        if (ogDesc != null && !ogDesc.isEmpty()) {
            return ogDesc;
        }

        // Try standard meta description
        String metaDesc = extractMetaTag(html, lower, "name", "description");
        if (metaDesc != null && !metaDesc.isEmpty()) {
            return metaDesc;
        }

        // Try Twitter card description
        String twitterDesc = extractMetaTag(html, lower, "name", "twitter:description");
        if (twitterDesc != null && !twitterDesc.isEmpty()) {
            return twitterDesc;
        }

        return null;
    }

    /** Extract content from a specific meta tag. */
    private static String extractMetaTag(String html, String lower, String attribute, String value) {
        int pos = 0;
        String searchPattern = "<meta";
        
        while ((pos = lower.indexOf(searchPattern, pos)) != -1) {
            int tagEnd = lower.indexOf(">", pos);
            if (tagEnd == -1) break;

            String tagContent = html.substring(pos, tagEnd + 1);
            String tagLower = tagContent.toLowerCase(Locale.ROOT);

            // Check if this meta tag has the correct attribute and value
            String attrPattern = attribute + "=\"" + value + "\"";
            String attrPatternSingle = attribute + "='" + value + "'";
            
            if (tagLower.contains(attrPattern) || tagLower.contains(attrPatternSingle)) {
                // Extract the content attribute
                int contentPos = tagLower.indexOf("content=");
                if (contentPos != -1) {
                    int contentStart = contentPos + 8; // length of "content="
                    if (contentStart < tagContent.length()) {
                        char quote = tagContent.charAt(contentStart);
                        if (quote == '"' || quote == '\'') {
                            int contentEnd = tagContent.indexOf(quote, contentStart + 1);
                            if (contentEnd != -1) {
                                String content = tagContent.substring(contentStart + 1, contentEnd);
                                content = content.replace("&nbsp;", " ")
                                                 .replace("&amp;", "&")
                                                 .replace("&lt;", "<")
                                                 .replace("&gt;", ">")
                                                 .replace("&quot;", "\"")
                                                 .replace("&#39;", "'");
                                return content.trim();
                            }
                        }
                    }
                }
            }

            pos = tagEnd + 1;
        }

        return null;
    }

    /** Extract content from semantic tags: <main>, <article>, <section>. */
    private static String extractSemanticContent(String html) {
        if (html == null) return null;
        String lower = html.toLowerCase(Locale.ROOT);

        // Try <main> tag
        int start = lower.indexOf("<main");
        if (start != -1) {
            int end = lower.indexOf("</main>", start);
            if (end != -1) {
                int contentStart = html.indexOf(">", start) + 1;
                return html.substring(contentStart, end);
            }
        }

        // Try <article> tag
        start = lower.indexOf("<article");
        if (start != -1) {
            int end = lower.indexOf("</article>", start);
            if (end != -1) {
                int contentStart = html.indexOf(">", start) + 1;
                return html.substring(contentStart, end);
            }
        }

        // Try <section> tag
        start = lower.indexOf("<section");
        if (start != -1) {
            int end = lower.indexOf("</section>", start);
            if (end != -1) {
                int contentStart = html.indexOf(">", start) + 1;
                return html.substring(contentStart, end);
            }
        }

        return null;
    }

    /** Strip markup and decode simple entities. */
    private static String extractBodyText(String html) {
        if (html == null) return "";

        String text = html;

        // (?is) -> case-insensitive + DOTALL
        text = text.replaceAll("(?is)<script[^>]*>.*?</script>", " ");
        text = text.replaceAll("(?is)<style[^>]*>.*?</style>", " ");
        text = text.replaceAll("(?is)<!--.*?-->", " ");
        text = text.replaceAll("(?is)<head[^>]*>.*?</head>", " ");

        // Remove navigation containers
        text = text.replaceAll("(?is)<nav[^>]*>.*?</nav>", " ");
        text = text.replaceAll("(?is)<header[^>]*>.*?</header>", " ");
        text = text.replaceAll("(?is)<footer[^>]*>.*?</footer>", " ");
        text = text.replaceAll("(?is)<aside[^>]*>.*?</aside>", " ");

        text = text.replaceAll("(?is)<[^>]+>", " ");

        text = text.replace("&nbsp;", " ");
        text = text.replace("&amp;", "&");
        text = text.replace("&lt;", "<");
        text = text.replace("&gt;", ">");
        text = text.replace("&quot;", "\"");
        text = text.replace("&#39;", "'");

        text = text.replaceAll("\\s+", " ");
        return text.trim();
    }

    /** Simple text cleaner used for titles / snippets. */
    private static String cleanText(String text) {
        if (text == null) return "";
        String t = text.replaceAll("<[^>]+>", " ");
        t = t.replaceAll("\\s+", " ");
        return t.trim();
    }

    /** Parse <a> tags, normalize URLs, cap results. */
    private static Set<String> extractLinks(String sourceUrl, String html) {
        Set<String> links = new HashSet<>();
        if (html == null || sourceUrl == null) return links;

        String lower = html.toLowerCase(Locale.ROOT);
        int pos = 0;
        while ((pos = lower.indexOf("<a", pos)) != -1) {
            int tagEnd = lower.indexOf(">", pos);
            if (tagEnd == -1) break;

            int hrefPos = lower.indexOf("href=", pos);
            if (hrefPos == -1 || hrefPos > tagEnd) {
                pos = tagEnd + 1;
                continue;
            }

            int start = hrefPos + 5;
            if (start >= html.length()) {
                pos = tagEnd + 1;
                continue;
            }

            char first = html.charAt(start);
            int urlStart, urlEnd;
            if (first == '"' || first == '\'') {
                urlStart = start + 1;
                urlEnd = html.indexOf(first, urlStart);
                if (urlEnd == -1 || urlEnd > tagEnd) {
                    pos = tagEnd + 1;
                    continue;
                }
            } else {
                urlStart = start;
                int space = html.indexOf(' ', urlStart);
                int gt = html.indexOf('>', urlStart);
                urlEnd = (space == -1) ? gt : (gt == -1 ? space : Math.min(space, gt));
                if (urlEnd == -1 || urlEnd > tagEnd) urlEnd = tagEnd;
            }

            if (urlStart >= urlEnd) {
                pos = tagEnd + 1;
                continue;
            }

            String href = html.substring(urlStart, urlEnd).trim();
            if (href.isEmpty()) {
                pos = tagEnd + 1;
                continue;
            }

            String lowerHref = href.toLowerCase(Locale.ROOT);
            if (lowerHref.startsWith("#") ||
                lowerHref.startsWith("mailto:") ||
                lowerHref.startsWith("javascript:") ||
                lowerHref.startsWith("tel:") ||
                lowerHref.startsWith("data:") ||
                lowerHref.startsWith("ftp:")) {
                pos = tagEnd + 1;
                continue;
            }

            String normalized = normalizeUrl(href, sourceUrl);
            if (normalized != null && !normalized.equals(sourceUrl)) {
                links.add(normalized);

                // If we already have enough outlinks, stop scanning this page.
                if (links.size() >= MAX_OUT_LINKS) {
                    break;
                }
            }

            pos = tagEnd + 1;
        }

        return links;
    }

    /** Resolve relative links while keeping encoding untouched. */
    private static String normalizeUrl(String link, String base) {
        if (link == null || link.isEmpty() || base == null || base.isEmpty()) return null;
        try {
            java.net.URI baseUri = new java.net.URI(base);
            java.net.URI resolved = baseUri.resolve(link);

            String scheme = resolved.getScheme();
            String host = resolved.getHost();
            String path = resolved.getPath();
            int port = resolved.getPort();

            if (scheme == null || host == null) return null;
            if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                return null;
            }
            if (path == null || path.isEmpty()) path = "/";

            if (port == -1) {
                port = "https".equalsIgnoreCase(scheme) ? 443 : 80;
            }

            // Drop fragment
            return scheme.toLowerCase(Locale.ROOT)
                    + "://"
                    + host.toLowerCase(Locale.ROOT)
                    + ":" + port
                    + path;
        } catch (Exception e) {
            return null;
        }
    }
}
