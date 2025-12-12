package frontend.storage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;

// Provides KVS-backed lookups for search metadata.
public class KVSStorage {

    private static KVSClient kvs;

    // KVS coordinator address (update if needed for deployment)
    private static final String KVS_COORDINATOR_ADDRESS = "localhost:8000";

    // Table names
    private static final String INDEX_TABLE    = "pt-index";
    private static final String PAGERANK_TABLE = "pt-pageranks";
    private static final String TFIDF_TABLE    = "pt-tfidf";
    private static final String TFIDF_PROGRESS_TABLE = "pt-tfidf-progress";
    private static final String CRAWL_TABLE    = "pt-crawl";
    private static final String INDEX_CHUNK_PREFIX = "chunk";
    private static final String INDEX_META_COUNT = "__count";
    private static final char INDEX_SEPARATOR = ',';

    private static final String TFIDF_PROGRESS_META_ROW = "__meta__";
    private static final String TFIDF_PROGRESS_TOTAL_COL = "totalDocs";

    static {
        try {
            kvs = new KVSClient(KVS_COORDINATOR_ADDRESS);
            System.out.println("[KVSStorage] Connected to KVS: " + KVS_COORDINATOR_ADDRESS);
        } catch (Exception e) {
            System.err.println("[KVSStorage] Failed to connect to KVS: " + e.getMessage());
            e.printStackTrace();
            kvs = null;
        }
    }

    // Returns URLs stored under the given index term.
    public static List<String> searchIndex(String word) {
        List<String> urlsList = new ArrayList<>();

        if (kvs == null || word == null || word.isEmpty()) {
            System.err.println("[KVSStorage] searchIndex: kvs null or empty word");
            return urlsList;
        }

        try {
            Row row = kvs.getRow(INDEX_TABLE, word.toLowerCase());
            if (row == null) {
                return urlsList;
            }

            int expected = parseIntSafe(row.get(INDEX_META_COUNT), 0);
            Set<String> orderedUrls = new LinkedHashSet<>();

            int chunkIndex = 0;
            while (true) {
                if (expected > 0 && orderedUrls.size() >= expected) {
                    break;
                }

                String column = chunkColumnName(chunkIndex++);
                String chunkValue = row.get(column);
                if (chunkValue == null || chunkValue.isEmpty()) {
                    break;
                }

                String[] urlArray = chunkValue.split(String.valueOf(INDEX_SEPARATOR));
                for (String u : urlArray) {
                    String trimmed = u.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    orderedUrls.add(trimmed);
                    if (expected > 0 && orderedUrls.size() >= expected) {
                        break;
                    }
                }
            }

            urlsList.addAll(orderedUrls);
        } catch (Exception e) {
            System.err.println("[KVSStorage] Error searching index for word: " + word);
            e.printStackTrace();
        }

        return urlsList;
    }

    // Loads pagerank, title, and snippet metadata for a URL.
    public static Object[] getPageRankMetadata(String url) {
        if (kvs == null || url == null || url.isEmpty()) {
            return null;
        }

        try {
            String urlHash = Hasher.hash(url);
            Row row = kvs.getRow(PAGERANK_TABLE, urlHash);
            
            if (row == null) {
                return null;
            }

            double pagerank = 0.1;  // default
            String title = url;      // fallback to URL
            String snippet = "...";  // default
            
            // Extract pagerank
            String prStr = row.get("pagerank");
            if (prStr != null) {
                try {
                    pagerank = Double.parseDouble(prStr.trim());
                } catch (NumberFormatException e) {
                    pagerank = 0.1;
                }
            }
            
            // Extract title
            String titleStr = row.get("title");
            if (titleStr != null && !titleStr.trim().isEmpty()) {
                title = titleStr.trim();
            }
            
            // Extract snippet
            String snippetStr = row.get("snippet");
            if (snippetStr != null && !snippetStr.trim().isEmpty()) {
                snippet = snippetStr.trim();
            }
            
            return new Object[] { pagerank, title, snippet };
            
        } catch (Exception e) {
            System.err.println("[KVSStorage] Error getting page rank metadata for URL: " + url);
        }

        return null;
    }

    // Computes the average TF-IDF score for the URL and query terms.
    public static double getTfIdf(String url, List<String> queryWords) {
        List<TfIdfComponent> components = getTfIdfComponents(url, queryWords);
        if (components.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (TfIdfComponent component : components) {
            sum += component.getTfidf();
        }
        return sum / components.size();
    }

    // Retrieve per-word TF/IDF components for diagnostics and weighting.
    public static List<TfIdfComponent> getTfIdfComponents(String url, List<String> queryWords) {
        if (kvs == null || url == null || url.isEmpty() ||
            queryWords == null || queryWords.isEmpty()) {
            return Collections.emptyList();
        }

        List<TfIdfComponent> components = new ArrayList<>();
        for (String word : queryWords) {
            if (word == null || word.isEmpty()) {
                continue;
            }
            try {
                String composite = word.toLowerCase(Locale.ROOT) + "|" + url;
                String key = Hasher.hash(composite);
                Row row = kvs.getRow(TFIDF_TABLE, key);
                if (row == null) {
                    continue;
                }
                double tf = parseDoubleSafe(row.get("tf"), 0.0);
                double idf = parseDoubleSafe(row.get("idf"), 0.0);
                double tfidf = parseDoubleSafe(row.get("tfidf"), 0.0);
                if (tfidf <= 0.0) {
                    continue;
                }
                components.add(new TfIdfComponent(word.toLowerCase(Locale.ROOT), tf, idf, tfidf));
            } catch (Exception e) {
                System.err.println("[KVSStorage] TF-IDF component lookup failed for word='" + word + "' url='" + url + "'");
            }
        }
        return components;
    }

    // Report TF-IDF job progress (best-effort).
    public static TfIdfStatus getTfIdfStatus() {
        if (kvs == null) {
            return TfIdfStatus.unavailable();
        }

        int totalDocs = 0;
        try {
            byte[] totalBytes = kvs.get(TFIDF_PROGRESS_TABLE, TFIDF_PROGRESS_META_ROW, TFIDF_PROGRESS_TOTAL_COL);
            if (totalBytes != null) {
                totalDocs = Integer.parseInt(new String(totalBytes, StandardCharsets.UTF_8).trim());
            }
        } catch (Exception ignored) {}

        int processedDocs = 0;
        try {
            int rawCount = kvs.count(TFIDF_PROGRESS_TABLE);
            processedDocs = Math.max(0, rawCount - 1); // subtract sentinel row
        } catch (Exception e) {
            // If count fails, fall back to unknown progress
            return new TfIdfStatus(totalDocs, -1, false);
        }

        return new TfIdfStatus(totalDocs, processedDocs, true);
    }

    // Retrieves the cached HTML page for a URL.
    public static String getCachedPage(String url) {
        if (kvs == null || url == null || url.isEmpty()) {
            return null;
        }

        try {
            String key = Hasher.hash(url);
            byte[] data = kvs.get(CRAWL_TABLE, key, "page");
            if (data != null) {
                return new String(data, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            System.err.println("[KVSStorage] Error getting cached page for URL: " + url);
            e.printStackTrace();
        }

        return null;
    }

    // Scans all index terms for spell suggestions.
    public static List<String> getAllIndexWords() {
        List<String> words = new ArrayList<>();

        if (kvs == null) {
            System.err.println("[KVSStorage] getAllIndexWords: kvs is null");
            return words;
        }

        try {
            Iterator<Row> it = kvs.scan(INDEX_TABLE);
            while (it.hasNext()) {
                Row r = it.next();
                String key = r.key();  // row key is the word
                if (key != null && !key.isEmpty()) {
                    words.add(key);
                }
            }
        } catch (Exception e) {
            System.err.println("[KVSStorage] Error scanning index table for words");
            e.printStackTrace();
        }

        return words;
    }

    // Reinitializes the KVS client.
    public static void init(String coordinatorAddress) {
        try {
            kvs = new KVSClient(coordinatorAddress);
            System.out.println("[KVSStorage] Reinitialized KVS: " + coordinatorAddress);
        } catch (Exception e) {
            System.err.println("[KVSStorage] Failed to reinitialize KVS: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String chunkColumnName(int index) {
        return INDEX_CHUNK_PREFIX + String.format("%04d", Math.max(index, 0));
    }

    private static int parseIntSafe(String value, int fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static double parseDoubleSafe(String value, double fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    // Container for TF-IDF breakdown per query term.
    public static class TfIdfComponent {
        private final String word;
        private final double tf;
        private final double idf;
        private final double tfidf;

        public TfIdfComponent(String word, double tf, double idf, double tfidf) {
            this.word = word;
            this.tf = tf;
            this.idf = idf;
            this.tfidf = tfidf;
        }

        public String getWord() {
            return word;
        }

        public double getTf() {
            return tf;
        }

        public double getIdf() {
            return idf;
        }

        public double getTfidf() {
            return tfidf;
        }
    }

    // Lightweight status summary for TF-IDF job progress.
    public static class TfIdfStatus {
        private final int totalDocs;
        private final int processedDocs;
        private final boolean progressKnown;

        private TfIdfStatus(int totalDocs, int processedDocs, boolean progressKnown) {
            this.totalDocs = Math.max(totalDocs, 0);
            this.processedDocs = processedDocs;
            this.progressKnown = progressKnown;
        }

        public static TfIdfStatus unavailable() {
            return new TfIdfStatus(0, -1, false);
        }

        public int getTotalDocs() {
            return totalDocs;
        }

        public int getProcessedDocs() {
            return processedDocs;
        }

        public boolean isProgressKnown() {
            return progressKnown && processedDocs >= 0;
        }

        public boolean isBuilding() {
            return isProgressKnown() && totalDocs > 0 && processedDocs < totalDocs;
        }

        public double getCompletionRatio() {
            if (!isProgressKnown() || totalDocs <= 0) {
                return 1.0;
            }
            double ratio = processedDocs / (double) totalDocs;
            if (ratio < 0.0) {
                return 0.0;
            }
            if (ratio > 1.0) {
                return 1.0;
            }
            return ratio;
        }

        public String toSummary() {
            if (!isProgressKnown()) {
                return "unknown";
            }
            return processedDocs + "/" + totalDocs;
        }
    }
}
