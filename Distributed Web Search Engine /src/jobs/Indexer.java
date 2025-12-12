package jobs;

import flame.FlameContext;
import flame.FlameRDD;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kvs.KVSClient;
import kvs.Row;
import tools.Logger;
import tools.StopWords;

public class Indexer {
    private static final Logger logger = Logger.getLogger(Indexer.class);
    private static final int MAX_URLS_PER_WORD = 100;
    private static final String INDEX_CHUNK_PREFIX = "chunk";
    private static final int INDEX_CHUNK_SIZE = 100;
    private static final double MAX_DOC_FRACTION = 0.15;
    private static final int MIN_WORD_FREQUENCY_PER_DOCUMENT = 2;
    private static final int MAX_HTML_BYTES = 20_000;
    private static final String PROGRESS_TABLE = "pt-index-progress";
    private static final String PROGRESS_SENTINEL_ROW = "__meta__";
    private static final String PROGRESS_SENTINEL_COL = "sentinel";

    public static void run(FlameContext context, String[] args) throws Exception {
        logger.info("Indexer job started with incremental word-url appends");
        context.output("Indexer: Loading crawl table and building index incrementally...");

        final KVSClient kvs = context.getKVS();

        boolean resumeEnabled = hasProgressMarker(kvs);

        if (!resumeEnabled) {
            context.output("Indexer: No resume marker found → fresh rebuild.");
            try {
                kvs.delete("pt-index");
                context.output("Indexer: Cleared existing pt-index table.");
            } catch (Exception e) {
                logger.warn("Unable to clear pt-index before rebuild: " + e.getMessage());
            }
            try {
                kvs.delete(PROGRESS_TABLE);
            } catch (Exception ignored) {
                // Table may not exist yet – safe to ignore
            }
            try {
                kvs.put(PROGRESS_TABLE, PROGRESS_SENTINEL_ROW, PROGRESS_SENTINEL_COL, Long.toString(System.currentTimeMillis()));
            } catch (Exception e) {
                logger.warn("Unable to seed progress sentinel: " + e.getMessage());
            }
        } else {
            context.output("Indexer: Resume marker detected → continuing without wiping pt-index.");
        }

        FlameRDD crawlData = context.fromTable("pt-crawl", rowObj -> {
            Row r = (Row) rowObj;
            String url = r.get("url");
            byte[] pageBytes = r.getBytes("page");
            if (url == null || pageBytes == null || pageBytes.length < 100) {
                return null;
            }

            int length = pageBytes.length;
            if (length > MAX_HTML_BYTES) {
                length = MAX_HTML_BYTES;
            }

            String page = new String(pageBytes, 0, length, StandardCharsets.UTF_8);
            return url + "\t" + page;
        }).filter(s -> s != null);

        int totalPages = 0;
        try {
            totalPages = kvs.count("pt-crawl");
        } catch (Exception e) {
            logger.warn("Unable to count pt-crawl rows via coordinator: " + e.getMessage());
        }
        if (totalPages <= 0) {
            totalPages = 1;
        }

        int docFrequencyCap = (int) Math.floor(totalPages * MAX_DOC_FRACTION);
        if (docFrequencyCap <= 0) {
            docFrequencyCap = MAX_URLS_PER_WORD;
        }
        docFrequencyCap = Math.min(MAX_URLS_PER_WORD, docFrequencyCap);
        docFrequencyCap = Math.max(1, docFrequencyCap);

        context.output("Indexer: Crawl rows=" + totalPages + ", doc-frequency cap=" + docFrequencyCap);
        context.output("Indexer: Using chunk size=" + INDEX_CHUNK_SIZE + ", min word freq=" + MIN_WORD_FREQUENCY_PER_DOCUMENT);
        context.output("Indexer: Writing chunked word-url pairs to pt-index incrementally (worker-side)...");
        if (resumeEnabled) {
            context.output("Indexer: Resume mode active; skipping pages already recorded in progress table.");
        }

        final String kvsCoordAddr = kvs.getCoordinator();
        crawlData.forEach(new PageToIndexAppender(
                kvsCoordAddr,
                MAX_URLS_PER_WORD,
                docFrequencyCap,
                INDEX_CHUNK_PREFIX,
                INDEX_CHUNK_SIZE,
                MIN_WORD_FREQUENCY_PER_DOCUMENT,
                PROGRESS_TABLE,
                resumeEnabled));

        logger.info("pt-index built successfully via incremental appends");
        context.output("Indexer: Done. pt-index built incrementally.");
        context.output("OK");
    }

    private static boolean hasProgressMarker(KVSClient kvs) {
        try {
            byte[] marker = kvs.get(PROGRESS_TABLE, PROGRESS_SENTINEL_ROW, PROGRESS_SENTINEL_COL);
            return marker != null;
        } catch (Exception e) {
            return false;
        }
    }

    // Helpers copied/adapted from PageRank for consistent text extraction
    private static String extractBodyTextForIndex(String html) {
        if (html == null) return "";
        // Remove script and style blocks
        html = html.replaceAll("(?i)<script[^>]*>.*?</script>", " ");
        html = html.replaceAll("(?i)<style[^>]*>.*?</style>", " ");
        // Optionally narrow to <body>
        int bodyStart = html.toLowerCase().indexOf("<body");
        if (bodyStart != -1) {
            int closingBracket = html.indexOf(">", bodyStart);
            if (closingBracket != -1 && closingBracket < html.length() - 1) {
                bodyStart = closingBracket + 1;
                int bodyEnd = html.toLowerCase().indexOf("</body>", bodyStart);
                if (bodyEnd != -1 && bodyEnd <= html.length()) {
                    html = html.substring(bodyStart, bodyEnd);
                }
            }
        }
        // Strip all tags
        html = html.replaceAll("<[^>]+>", " ");
        // Decode common entities
        html = html.replace("&nbsp;", " ");
        html = html.replace("&amp;", "&");
        html = html.replace("&lt;", "<");
        html = html.replace("&gt;", ">");
        // Collapse whitespace
        html = html.replaceAll("\\s+", " ").trim();
        return html;
    }

    private static class PageToIndexAppender implements FlameRDD.StringToVoid {
        private final String kvsCoordinator;
        private final int maxUrlsPerWord;
        private final int docFrequencyCap;
        private final String chunkPrefix;
        private final int chunkSize;
        private final int minWordFrequency;
        private final String progressTable;
        private final boolean resumeEnabled;
        private transient KVSClient workerKvs;

        PageToIndexAppender(String kvsCoordinator,
                            int maxUrlsPerWord,
                            int docFrequencyCap,
                            String chunkPrefix,
                            int chunkSize,
                            int minWordFrequency,
                            String progressTable,
                            boolean resumeEnabled) {
            this.kvsCoordinator = kvsCoordinator;
            this.maxUrlsPerWord = Math.max(1, maxUrlsPerWord);
            this.docFrequencyCap = Math.max(0, docFrequencyCap);
            this.chunkPrefix = (chunkPrefix == null || chunkPrefix.isEmpty()) ? "chunk" : chunkPrefix;
            this.chunkSize = Math.max(1, chunkSize);
            this.minWordFrequency = Math.max(1, minWordFrequency);
            this.progressTable = progressTable;
            this.resumeEnabled = resumeEnabled;
        }

        @Override
        public void op(String record) throws Exception {
            if (record == null || record.isEmpty()) {
                return;
            }

            int tabIndex = record.indexOf('\t');
            if (tabIndex <= 0 || tabIndex >= record.length() - 1) {
                return;
            }

            String url = record.substring(0, tabIndex);
            String page = record.substring(tabIndex + 1);
            if (url.isEmpty() || page.isEmpty()) {
                return;
            }
            if (!(url.startsWith("http://") || url.startsWith("https://"))) {
                return;
            }

            String decodedUrl = url;
            try {
                decodedUrl = java.net.URLDecoder.decode(url, java.nio.charset.StandardCharsets.UTF_8);
            } catch (Exception ignored) {
                // Keep original URL if decoding fails
            }

            String text = extractBodyTextForIndex(page);
            if (text.isEmpty()) {
                return;
            }

            String[] tokens = text.toLowerCase().split("[^a-z]+");
            Map<String, Integer> wordCounts = new HashMap<>();
            for (String token : tokens) {
                if (token == null || token.isEmpty()) {
                    continue;
                }
                if (token.length() > 25) {
                    continue;
                }
                if (StopWords.isStopWord(token)) {
                    continue;
                }
                wordCounts.merge(token, 1, Integer::sum);
            }

            if (wordCounts.isEmpty()) {
                return;
            }

            Set<String> wordsToIndex = new HashSet<>();
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                if (entry.getValue() >= minWordFrequency) {
                    wordsToIndex.add(entry.getKey());
                }
            }

            if (wordsToIndex.isEmpty()) {
                return;
            }

            if (workerKvs == null) {
                workerKvs = new KVSClient(kvsCoordinator);
            }

            if (resumeEnabled && alreadyProcessed(decodedUrl)) {
                return;
            }

            for (String word : wordsToIndex) {
                try {
                    // Worker-side client chunks posting list writes and enforces document caps.
                    workerKvs.appendCapped(
                            "pt-index",
                            word,
                            chunkPrefix,
                            decodedUrl,
                            maxUrlsPerWord,
                            ',',
                            chunkSize,
                            docFrequencyCap);
                } catch (Exception e) {
                    logger.error("Error appending to pt-index for word '" + word + "': " + e.getMessage());
                }
            }

            try {
                workerKvs.put(progressTable, decodedUrl, "done", "1");
            } catch (Exception e) {
                logger.warn("Failed to mark progress for URL '" + decodedUrl + "': " + e.getMessage());
            }
        }

        private boolean alreadyProcessed(String decodedUrl) {
            try {
                byte[] value = workerKvs == null
                        ? null
                        : workerKvs.get(progressTable, decodedUrl, "done");
                if (value != null) {
                    return true;
                }
            } catch (Exception e) {
                logger.warn("Progress lookup failed for URL '" + decodedUrl + "': " + e.getMessage());
            }
            return false;
        }
    }
}
