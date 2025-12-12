package frontend.handler;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import frontend.app.Result;
import frontend.storage.KVSStorage;
import frontend.storage.KVSStorage.TfIdfComponent;
import frontend.storage.KVSStorage.TfIdfStatus;
import frontend.tools.SpellChecker;
import frontend.view.TemplateEngine;
import tools.StopWords;
import webserver.Request;
import webserver.Response;
import webserver.Route;

// Handles search results.
public class ResultPageHandler implements Route {

    private static final int RESULTS_PER_PAGE = 10;

    @Override
    public Object handle(Request req, Response res) throws Exception {

        long startTime = System.nanoTime();

        String rawQuery = req.queryParams("query");
        if (rawQuery == null) rawQuery = "";

        boolean isDiagMode = "true".equals(req.queryParams("diag"));

        // ajax=1 -> results fragment
        boolean isAjax = "1".equals(req.queryParams("ajax"));

        // Paging
        int currentPage = 1;
        String pageParam = req.queryParams("page");
        if (pageParam != null && !pageParam.isEmpty()) {
            try {
                currentPage = Integer.parseInt(pageParam);
            } catch (NumberFormatException ignored) {}
        }
        if (currentPage < 1) currentPage = 1;

        // Geo coords
        double userLat = Double.NaN;
        double userLon = Double.NaN;
        String latParam = req.queryParams("lat");
        String lonParam = req.queryParams("lon");
        try {
            if (latParam != null && !latParam.isEmpty()) {
                userLat = Double.parseDouble(latParam);
            }
        } catch (NumberFormatException ignored) {}
        try {
            if (lonParam != null && !lonParam.isEmpty()) {
                userLon = Double.parseDouble(lonParam);
            }
        } catch (NumberFormatException ignored) {}

        // Resolve city name
        String geoCityName = resolveCityName(userLat, userLon);
        String cityKeyword = (geoCityName == null ? null : geoCityName.toLowerCase());

        // Tokenize
        List<String> queryWords = filterStopWords(rawQuery);

        TfIdfStatus tfIdfStatus = KVSStorage.getTfIdfStatus();
        double tfIdfCompletion = 1.0;
        boolean tfIdfBuilding = false;
        if (tfIdfStatus != null) {
            tfIdfCompletion = tfIdfStatus.getCompletionRatio();
            tfIdfBuilding = tfIdfStatus.isBuilding();
            if (tfIdfStatus.isProgressKnown()) {
                res.header("X-TfIdf-Progress", tfIdfStatus.toSummary());
            }
        }

        // Spellcheck
        String didYouMean = null;
        if (!queryWords.isEmpty()) {
            didYouMean = SpellChecker.suggestQuery(queryWords);
            if (didYouMean != null &&
                didYouMean.trim().equalsIgnoreCase(rawQuery.trim())) {
                didYouMean = null;
            }
        }

        // Candidate URLs
        List<String> allUrls = searchUrlsForWords(queryWords);

        // Rank results
        List<Result> allResults = new ArrayList<>();

        for (String url : allUrls) {

            // Core signals
            double tfidf = KVSStorage.getTfIdf(url, queryWords);

            // Metadata fetch
            Object[] metadata = KVSStorage.getPageRankMetadata(url);
            double pagerank;
            String title;
            String snippet;

            if (metadata != null) {
                pagerank = (Double) metadata[0];
                title    = (String) metadata[1];
                snippet  = (String) metadata[2];
            } else {
                // Metadata fallback
                pagerank = 0.1;
                title    = url;
                snippet  = "...";
            }

            // Derived signals
            double prScore = Math.log(1.0 + Math.max(pagerank, 0.0));
            double tfScore = Math.max(tfidf, 0.0);
            if (tfIdfCompletion < 1.0) {
                tfScore *= tfIdfCompletion;
            }

            String lowerTitle = (title == null ? "" : title.toLowerCase());
            String lowerUrl   = url.toLowerCase();

            int titleMatches = 0;
            int urlMatches   = 0;

            for (String w : queryWords) {
                if (w == null || w.isEmpty()) continue;
                if (lowerTitle.contains(w)) {
                    titleMatches++;
                }
                if (lowerUrl.contains(w)) {
                    urlMatches++;
                }
            }

            double titleBoost = 0.0;
            double urlBoost   = 0.0;
            if (!queryWords.isEmpty()) {
                titleBoost = (double) titleMatches / (double) queryWords.size();
                urlBoost   = (double) urlMatches   / (double) queryWords.size();
            }

            // Geo boost
            double geoBoost = 0.0;
            if (cityKeyword != null) {
                String lowerSnippet = (snippet == null ? "" : snippet.toLowerCase());
                geoBoost = computeGeoBoost(cityKeyword, lowerTitle, lowerUrl, lowerSnippet);
            }

            // Short URL bonus
            double lengthPenalty = 1.0;
            int urlLen = url.length();
            if (urlLen > 120) {
                lengthPenalty = 1.0 / (1.0 + (urlLen - 120) / 60.0);
            }

            double finalScore =
                    0.45 * prScore +
                    0.45 * tfScore +
                    0.07 * titleBoost +
                    0.03 * urlBoost;

            if (geoBoost > 0.0) {
                finalScore *= (1.0 + 0.2 * geoBoost);
            }

            finalScore *= lengthPenalty;

            Result r = new Result(url, title, snippet, pagerank, tfidf, finalScore);
            r.setGeoBoost(geoBoost);
            r.setGeoCity(geoCityName);
            r.setTfIdfIncomplete(tfIdfBuilding);
            r.setTfIdfScale(tfIdfCompletion);
            if (isDiagMode) {
                List<TfIdfComponent> components = KVSStorage.getTfIdfComponents(url, queryWords);
                r.setTfIdfDetails(components);
            }
            allResults.add(r);
        }

        // Sort & dedupe
        allResults.sort((r1, r2) -> Double.compare(r2.getFinalScore(), r1.getFinalScore()));

        int beforeDedup = allResults.size();
        allResults = deduplicateByTitle(allResults);
        int afterDedup = allResults.size();
        
        if (beforeDedup != afterDedup) {
            System.out.println("[DEBUG] Deduplication: " + beforeDedup + " -> " + afterDedup + 
                             " (removed " + (beforeDedup - afterDedup) + " duplicates)");
        }

        // Page slice
        int totalResults = allResults.size();
        int totalPages   = (int) Math.ceil((double) totalResults / RESULTS_PER_PAGE);
        if (totalPages == 0) totalPages = 1;

        if (currentPage > totalPages) {
            currentPage = totalPages;
        }

        int startIndex = (currentPage - 1) * RESULTS_PER_PAGE;
        if (startIndex < 0) startIndex = 0;
        int endIndex   = Math.min(startIndex + RESULTS_PER_PAGE, totalResults);

        List<Result> resultsForThisPage = new ArrayList<>();
        if (startIndex < totalResults) {
            resultsForThisPage = allResults.subList(startIndex, endIndex);
        }

        boolean hasMore = currentPage < totalPages;

        long endTime = System.nanoTime();
        double totalQueryTime = (endTime - startTime) / 1_000_000_000.0;

        res.type("text/html");

        // AJAX fragment
        if (isAjax) {
            return TemplateEngine.renderResultsFragment(resultsForThisPage, isDiagMode, geoCityName);
        }

        // Render page
        return TemplateEngine.renderResultPage(
                rawQuery,
                didYouMean,
                resultsForThisPage,
                currentPage,
                totalPages,
                isDiagMode,
                totalResults,
                totalQueryTime,
                hasMore,
                geoCityName
        );
    }

    // Attempts reverse geocoding with fallback.
    private String resolveCityName(double lat, double lon) {
        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            return null;
        }

        String city = reverseGeocodeCity(lat, lon);
        if (city != null && !city.trim().isEmpty()) {
            return city.trim();
        }

        return inferCityKeywordFromLocation(lat, lon);
    }

    // Uses Nominatim for city lookup.
    @SuppressWarnings("deprecation")
    private String reverseGeocodeCity(double lat, double lon) {
        try {
            String urlStr = "https://nominatim.openstreetmap.org/reverse?format=jsonv2"
                    + "&lat=" + URLEncoder.encode(Double.toString(lat), "UTF-8")
                    + "&lon=" + URLEncoder.encode(Double.toString(lon), "UTF-8")
                    + "&zoom=10&addressdetails=1";

            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "MegaSearch/1.0 (contact@example.com)");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);

            int code = conn.getResponseCode();
            if (code != 200) {
                return null;
            }

            try (InputStream in = conn.getInputStream();
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

                byte[] buf = new byte[4096];
                int len;
                while ((len = in.read(buf)) != -1) {
                    baos.write(buf, 0, len);
                }
                String json = new String(baos.toByteArray(), StandardCharsets.UTF_8);
                return extractCityFromNominatimJson(json);
            }
        } catch (Exception e) {
            return null;
        }
    }

    // Pulls city-like field from JSON.
    private String extractCityFromNominatimJson(String json) {
        if (json == null || json.isEmpty()) return null;

        String[] keys = {
                "\"city\":\"",
                "\"town\":\"",
                "\"village\":\"",
                "\"municipality\":\"",
                "\"county\":\""
        };

        for (String key : keys) {
            int idx = json.indexOf(key);
            if (idx >= 0) {
                int start = idx + key.length();
                int end = json.indexOf('"', start);
                if (end > start) {
                    String name = json.substring(start, end);
                    if (!name.isEmpty()) {
                        return name;
                    }
                }
            }
        }
        return null;
    }

    // Bounding-box fallback.
    private String inferCityKeywordFromLocation(double lat, double lon) {
        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            return null;
        }

        // Rough NYC bounding box: ~40.3–41.1 N, -74.5–-73.3 W
        if (lat > 40.3 && lat < 41.1 && lon > -74.5 && lon < -73.3) {
            return "New York";
        }
        // Los Angeles
        if (lat > 33.7 && lat < 34.4 && lon > -119.0 && lon < -117.5) {
            return "Los Angeles";
        }
        // San Francisco
        if (lat > 37.70 && lat < 37.83 && lon > -122.53 && lon < -122.35) {
            return "San Francisco";
        }

        // Oakland
        if (lat > 37.70 && lat < 37.90 && lon > -122.30 && lon < -122.10) {
            return "Oakland";
        }

        // San Jose
        if (lat > 37.20 && lat < 37.45 && lon > -122.05 && lon < -121.75) {
            return "San Jose";
        }

        // Philadelphia
        if (lat > 39.8 && lat < 40.2 && lon > -75.3 && lon < -74.8) {
            return "Philadelphia";
        }
        return null;
    }

    // Returns 1 on exact city match.
    private double computeGeoBoost(String cityKeyword,
                                   String lowerTitle,
                                   String lowerUrl,
                                   String lowerSnippet) {
        if (cityKeyword == null || cityKeyword.isEmpty()) {
            return 0.0;
        }

        if (lowerTitle != null && lowerTitle.contains(cityKeyword)) {
            return 1.0;
        }
        if (lowerUrl != null && lowerUrl.contains(cityKeyword)) {
            return 1.0;
        }
        if (lowerSnippet != null && lowerSnippet.contains(cityKeyword)) {
            return 1.0;
        }

        return 0.0;
    }

    // Optional normalization.
    @SuppressWarnings("unused")
    private static double normalizeToUnit(double x, double min, double max) {
        if (Double.isInfinite(min) || Double.isInfinite(max)) {
            return 0.0;
        }
        double range = max - min;
        if (range == 0.0) {
            return 0.0;
        }
        double v = (x - min) / range;
        if (v < 0.0) return 0.0;
        if (v > 1.0) return 1.0;
        return v;
    }

    // Intersect index hits.
    private List<String> searchUrlsForWords(List<String> queryWords) {
        List<String> allUrls = new ArrayList<>();

        if (queryWords == null || queryWords.isEmpty()) {
            return allUrls;
        }

        allUrls = new ArrayList<>(KVSStorage.searchIndex(queryWords.get(0)));

        for (int i = 1; i < queryWords.size() && !allUrls.isEmpty(); i++) {
            List<String> wordUrls = KVSStorage.searchIndex(queryWords.get(i));
            allUrls.retainAll(wordUrls);
        }

        return allUrls;
    }

    // Deduplicate by title.
    private List<Result> deduplicateByTitle(List<Result> results) {
        if (results == null || results.isEmpty()) {
            return results;
        }

        java.util.Map<String, Result> titleMap = new java.util.LinkedHashMap<>();

        for (Result r : results) {
            String title = r.getTitle();
            if (title == null || title.isEmpty()) {
                titleMap.put("__NO_TITLE__" + r.getUrl(), r);
                continue;
            }

            String normalizedTitle = title.trim().toLowerCase();

            // Keep the highest score
            if (!titleMap.containsKey(normalizedTitle)) {
                titleMap.put(normalizedTitle, r);
            } else {
                // System.out.println("[DEBUG] Skipping duplicate title: '" + title + "' for URL: " + r.getUrl());
            }
        }

        return new ArrayList<>(titleMap.values());
    }

    // Tokenize sans stopwords.
    private List<String> filterStopWords(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> rawWords =
                Arrays.asList(rawQuery.toLowerCase().split("\\s+"));

        return rawWords.stream()
                .filter(word -> !word.isEmpty() && !StopWords.isStopWord(word))
                .collect(Collectors.toList());
    }
}
