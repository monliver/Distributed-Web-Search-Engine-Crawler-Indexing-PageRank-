package jobs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import flame.FlameContext;
import flame.FlameRDD;
import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;
import tools.Logger;
import tools.StopWords;

/** Streaming TF-IDF builder with resume-aware progress tracking. */
public class TfIdf {

    private static final Logger logger = Logger.getLogger(TfIdf.class);

    private static final String TFIDF_TABLE = "pt-tfidf";
    private static final String INDEX_TABLE = "pt-index";
    private static final String PAGERANK_TABLE = "pt-pageranks";
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String PROGRESS_TABLE = "pt-tfidf-progress";
    private static final String PROGRESS_SENTINEL_ROW = "__meta__";
    private static final String PROGRESS_SENTINEL_COL = "sentinel";
    private static final String PROGRESS_TOTAL_COL = "totalDocs";
    private static final String PROGRESS_DONE_COL = "done";

    private static final int MAX_HTML_BYTES = 20_000;
    private static final int MAX_WORD_LENGTH = 25;
    private static final int MAX_TERMS_PER_DOCUMENT = 60;

    public static void run(FlameContext context, String[] args) throws Exception {
        context.output("========================================");
        context.output("TF-IDF Builder");
        context.output("========================================");

        final KVSClient kvs = context.getKVS();
        final boolean resumeEnabled = hasProgressMarker(kvs);

        if (!resumeEnabled) {
            context.output("TF-IDF: No progress marker found → fresh rebuild.");
            try {
                kvs.delete(TFIDF_TABLE);
                context.output("TF-IDF: Cleared existing " + TFIDF_TABLE + ".");
            } catch (Exception e) {
                logger.warn("Unable to clear " + TFIDF_TABLE + ": " + e.getMessage());
            }
            try {
                kvs.delete(PROGRESS_TABLE);
            } catch (Exception ignored) {
                // Table may not exist yet.
            }
            try {
                kvs.put(PROGRESS_TABLE, PROGRESS_SENTINEL_ROW, PROGRESS_SENTINEL_COL,
                        Long.toString(System.currentTimeMillis()));
            } catch (Exception e) {
                logger.warn("Unable to seed TF-IDF progress sentinel: " + e.getMessage());
            }
        } else {
            context.output("TF-IDF: Resume marker detected → continuing without wiping " + TFIDF_TABLE + ".");
        }

        int totalDocs = resolveTotalDocs(kvs, resumeEnabled);
        if (totalDocs <= 0) {
            totalDocs = 1;
        }
        final double totalDocsD = totalDocs;
        context.output("TF-IDF: Total documents for IDF math=" + totalDocs);
        try {
            kvs.put(PROGRESS_TABLE, PROGRESS_SENTINEL_ROW, PROGRESS_TOTAL_COL, Integer.toString(totalDocs));
        } catch (Exception e) {
            logger.warn("Unable to store totalDocs in progress table: " + e.getMessage());
        }

        FlameRDD crawlData = context.fromTable(CRAWL_TABLE, rowObj -> {
            Row row = (Row) rowObj;
            String url = row.get("url");
            byte[] pageBytes = row.getBytes("page");
            if (url == null || pageBytes == null || pageBytes.length < 64) {
                return null;
            }
            int length = Math.min(pageBytes.length, MAX_HTML_BYTES);
            String page = new String(pageBytes, 0, length, StandardCharsets.UTF_8);
            return url + "\t" + page;
        }).filter(s -> s != null);

        final String coordinator = kvs.getCoordinator();
        crawlData.forEach(new TfIdfComputer(coordinator, totalDocsD, resumeEnabled));

        context.output("========================================");
        context.output("TF-IDF job completed");
        context.output("========================================");
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

    private static int resolveTotalDocs(KVSClient kvs, boolean resumeEnabled) {
        try {
            byte[] cached = kvs.get(PROGRESS_TABLE, PROGRESS_SENTINEL_ROW, PROGRESS_TOTAL_COL);
            if (resumeEnabled && cached != null) {
                try {
                    return Integer.parseInt(new String(cached, StandardCharsets.UTF_8).trim());
                } catch (NumberFormatException ignored) {
                    // fall through
                }
            }
        } catch (Exception ignored) {}

        int count = 0;
        try {
            count = kvs.count(PAGERANK_TABLE);
        } catch (Exception e) {
            logger.warn("Unable to count " + PAGERANK_TABLE + ": " + e.getMessage());
        }
        if (count <= 0) {
            try {
                count = kvs.count(CRAWL_TABLE);
            } catch (Exception e) {
                logger.warn("Unable to count " + CRAWL_TABLE + ": " + e.getMessage());
            }
        }
        return count;
    }

    private static class TfIdfComputer implements FlameRDD.StringToVoid {

        private final String kvsCoordinator;
        private final double totalDocs;
        private final boolean resumeEnabled;
        private transient KVSClient workerKvs;
        private transient Map<String, Integer> docFreqCache;

        TfIdfComputer(String kvsCoordinator, double totalDocs, boolean resumeEnabled) {
            this.kvsCoordinator = kvsCoordinator;
            this.totalDocs = totalDocs <= 0 ? 1.0 : totalDocs;
            this.resumeEnabled = resumeEnabled;
        }

        @Override
        public void op(String record) throws Exception {
            if (record == null || record.isEmpty()) {
                return;
            }

            int tab = record.indexOf('\t');
            if (tab <= 0 || tab >= record.length() - 1) {
                return;
            }

            String url = record.substring(0, tab);
            String page = record.substring(tab + 1);
            if (url.isEmpty() || page.isEmpty()) {
                return;
            }
            if (!(url.startsWith("http://") || url.startsWith("https://"))) {
                return;
            }

            ensureWorkerClient();

            String decodedUrl = decodeUrl(url);
            if (resumeEnabled && alreadyProcessed(decodedUrl)) {
                return;
            }

            String body = extractBodyText(page);
            if (body.isEmpty()) {
                markProgress(decodedUrl);
                return;
            }

            Map<String, Integer> wordCounts = tokenize(body);
            if (wordCounts.isEmpty()) {
                markProgress(decodedUrl);
                return;
            }

            List<Entry<String, Integer>> entries = new ArrayList<>(wordCounts.entrySet());
            entries.removeIf(e -> e.getValue() == null || e.getValue() <= 0);
            if (entries.isEmpty()) {
                markProgress(decodedUrl);
                return;
            }

            entries.sort(Comparator.comparingInt(Entry<String, Integer>::getValue).reversed());
            int limit = Math.min(entries.size(), MAX_TERMS_PER_DOCUMENT);

            for (int i = 0; i < limit; i++) {
                Entry<String, Integer> entry = entries.get(i);
                String word = entry.getKey();
                int rawCount = entry.getValue();
                if (word == null || word.isEmpty() || rawCount <= 0) {
                    continue;
                }

                int docFreq = fetchDocFrequency(word);
                if (docFreq <= 0) {
                    continue;
                }

                double tf = 1.0 + Math.log(rawCount);
                double idf = Math.log((totalDocs + 1.0) / (docFreq + 1.0));
                if (idf <= 0.0) {
                    continue;
                }

                double tfidf = tf * idf;
                if (tfidf <= 0.0) {
                    continue;
                }

                String key = Hasher.hash(word + "|" + decodedUrl);
                Row row = new Row(key);
                row.put("word", word);
                row.put("url", decodedUrl);
                row.put("tf", Double.toString(tf));
                row.put("idf", Double.toString(idf));
                row.put("tfidf", Double.toString(tfidf));
                try {
                    workerKvs.putRow(TFIDF_TABLE, row);
                } catch (Exception e) {
                    logger.error("TF-IDF: Failed to write row for word='" + word + "', url='" + decodedUrl + "': " + e.getMessage());
                }
            }

            markProgress(decodedUrl);
        }

        private void ensureWorkerClient() {
            if (workerKvs == null) {
                workerKvs = new KVSClient(kvsCoordinator);
            }
            if (docFreqCache == null) {
                docFreqCache = new HashMap<>();
            }
        }

        private boolean alreadyProcessed(String decodedUrl) {
            try {
                byte[] value = workerKvs.get(PROGRESS_TABLE, decodedUrl, PROGRESS_DONE_COL);
                return value != null;
            } catch (Exception e) {
                logger.warn("TF-IDF: Progress lookup failed for URL '" + decodedUrl + "': " + e.getMessage());
                return false;
            }
        }

        private void markProgress(String decodedUrl) {
            try {
                workerKvs.put(PROGRESS_TABLE, decodedUrl, PROGRESS_DONE_COL, "1");
            } catch (Exception e) {
                logger.warn("TF-IDF: Failed to mark progress for URL '" + decodedUrl + "': " + e.getMessage());
            }
        }

        private int fetchDocFrequency(String word) {
            ensureWorkerClient();
            Integer cached = docFreqCache.get(word);
            if (cached != null) {
                return cached;
            }
            int docFreq = 0;
            try {
                byte[] raw = workerKvs.get(INDEX_TABLE, word, "__count");
                if (raw != null) {
                    docFreq = Integer.parseInt(new String(raw, StandardCharsets.UTF_8).trim());
                }
            } catch (NumberFormatException nfe) {
                docFreq = 0;
            } catch (Exception e) {
                logger.warn("TF-IDF: Failed to fetch doc frequency for word '" + word + "': " + e.getMessage());
            }
            docFreqCache.put(word, docFreq);
            return docFreq;
        }

        private Map<String, Integer> tokenize(String body) {
            if (body == null || body.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, Integer> counts = new HashMap<>();
            String[] tokens = body.toLowerCase(Locale.ROOT).split("[^a-z]+");
            for (String token : tokens) {
                if (token == null || token.isEmpty()) {
                    continue;
                }
                if (token.length() > MAX_WORD_LENGTH) {
                    continue;
                }
                if (StopWords.isStopWord(token)) {
                    continue;
                }
                counts.merge(token, 1, Integer::sum);
            }
            return counts;
        }

        private String decodeUrl(String url) {
            try {
                return java.net.URLDecoder.decode(url, StandardCharsets.UTF_8);
            } catch (Exception ignored) {
                return url;
            }
        }
    }

    private static String extractBodyText(String html) {
        if (html == null || html.isEmpty()) {
            return "";
        }

        String cleaned = html.replaceAll("(?is)<script[^>]*>.*?</script>", " ")
                              .replaceAll("(?is)<style[^>]*>.*?</style>", " ")
                              .replaceAll("(?is)<!--.*?-->", " ");

        int bodyStart = cleaned.toLowerCase(Locale.ROOT).indexOf("<body");
        if (bodyStart >= 0) {
            int close = cleaned.indexOf('>', bodyStart);
            if (close >= 0) {
                int bodyEnd = cleaned.toLowerCase(Locale.ROOT).indexOf("</body>", close + 1);
                if (bodyEnd > close) {
                    cleaned = cleaned.substring(close + 1, bodyEnd);
                }
            }
        }

        cleaned = cleaned.replaceAll("(?is)<(nav|header|footer|aside)[^>]*>.*?</\\1>", " ");
        cleaned = cleaned.replaceAll("<[^>]+>", " ");
        cleaned = cleaned.replace("&nbsp;", " ")
                         .replace("&amp;", "&")
                         .replace("&lt;", "<")
                         .replace("&gt;", ">");
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned;
    }
}
