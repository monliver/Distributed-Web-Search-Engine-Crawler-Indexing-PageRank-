package frontend.view;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.List;

import frontend.app.Result;
import frontend.storage.KVSStorage.TfIdfComponent;

public class TemplateEngine {

    // Legacy entry point without suggestion or pagination flags.
    public static String renderResultPage(
            String query,
            List<Result> results,
            int currentPage,
            int totalPages,
            boolean isDiagMode,
            int totalResults,
            double totalQueryTime) throws Exception {

        return renderResultPage(
                query,
                null,
                results,
                currentPage,
                totalPages,
                isDiagMode,
                totalResults,
                totalQueryTime,
                false,
                null
        );
    }

    // Full result page renderer with suggestion and paging data.
    public static String renderResultPage(
            String query,
            String didYouMean,
            List<Result> results,
            int currentPage,
            int totalPages,
            boolean isDiagMode,
            int totalResults,
            double totalQueryTime,
            boolean hasMore,
            String geoCityName) throws Exception {

        String template = FileUtil.readFromFile("frontend/static/resultPage.html");

        NumberFormat formatter = NumberFormat.getInstance();
        formatter.setMaximumFractionDigits(2);
        String summaryLine = String.format(
                "About %s results (%s seconds)",
                formatter.format(totalResults),
                formatter.format(totalQueryTime)
        );

        String resultsHtml = buildResultsHtml(results, isDiagMode, query);

        String didYouMeanHtml = buildDidYouMeanHtml(didYouMean);

        template = template.replace("{{query}}", htmlEscape(query));
        template = template.replace("{{summary}}", summaryLine);
        template = template.replace("{{results}}", resultsHtml);
        template = template.replace("{{pagination}}", "");
        template = template.replace("{{didyoumean}}", didYouMeanHtml);

        template = template.replace("{{currentPage}}", Integer.toString(currentPage));
        template = template.replace("{{totalPages}}", Integer.toString(totalPages));
        template = template.replace("{{diagMode}}", Boolean.toString(isDiagMode));

        return template;
    }

    // Renders only the result list for AJAX pagination.
    public static String renderResultsFragment(List<Result> results, boolean isDiagMode) {
        return buildResultsHtml(results, isDiagMode, null);
    }
    public static String renderResultsFragment(
            List<Result> results,
            boolean isDiagMode,
            String geoCityName
    ) {
        return renderResultsFragment(results, isDiagMode);
    }
    // Helper methods

    private static String buildResultsHtml(List<Result> results, boolean isDiagMode, String query) {
        StringBuilder sb = new StringBuilder();

        if (results == null || results.isEmpty()) {
            sb.append("<p>No results found.</p>");
            return sb.toString();
        }

        String highlightQuery = (query == null ? "" : query.trim());

        for (Result item : results) {
            String url = item.getUrl();
            String escUrlAttr = htmlAttributeEscape(url);
            String escUrlText = htmlEscape(url);
            String escTitle = htmlEscape(item.getTitle());
            String escSnippet = htmlEscape(item.getSnippet());

            // snippet highlight query
            if (!highlightQuery.isEmpty()) {
                String snippetLower = escSnippet.toLowerCase();
                String termLower = highlightQuery.toLowerCase();

                int n = snippetLower.length();
                int m = termLower.length();
                if (m > 0 && n >= m) {
                    StringBuilder highlighted = new StringBuilder();
                    int i = 0;
                    while (i <= n - m) {
                        if (!snippetLower.regionMatches(i, termLower, 0, m)) {
                            highlighted.append(escSnippet.charAt(i));
                            i++;
                            continue;
                        }

                        boolean leftOk;
                        if (i == 0) {
                            leftOk = true;
                        } else {
                            char lc = snippetLower.charAt(i - 1);
                            leftOk = !Character.isLetterOrDigit(lc);
                        }

                        boolean rightOk;
                        if (i + m == n) {
                            rightOk = true;
                        } else {
                            char rc = snippetLower.charAt(i + m);
                            rightOk = !Character.isLetterOrDigit(rc);
                        }

                        if (leftOk && rightOk) {
                            highlighted.append("<b>")
                                       .append(escSnippet, i, i + m)
                                       .append("</b>");
                            i += m;
                        } else {
                            highlighted.append(escSnippet.charAt(i));
                            i++;
                        }
                    }

                    if (i < n) {
                        highlighted.append(escSnippet.substring(i));
                    }

                    escSnippet = highlighted.toString();
                }
            }

            sb.append("<div class='result-item'>");

            // Title
            sb.append("<h3 class='result-title'>")
              .append("<a href='").append(escUrlAttr).append("'>")
              .append(escTitle)
              .append("</a></h3>");

            // URL + Cached link
            sb.append("<p class='result-url'>")
              .append("<a href='").append(escUrlAttr).append("'>")
              .append(escUrlText)
              .append("</a>")
              .append(" <a class='cached-link' href='/cache?url=")
              .append(URLEncoder.encode(url, StandardCharsets.UTF_8))
              .append("'>Cached</a>")
              .append("</p>");

            // Snippet
            sb.append("<p class='result-snippet'>")
              .append(escSnippet)
              .append("</p>");

            // Diagnostic info (PageRank, TF-IDF, final score, Geo)
            if (isDiagMode) {
                double geoBoost = item.getGeoBoost();
                String geoCity = item.getGeoCity();
                boolean geoOn = (geoCity != null && !geoCity.isEmpty());

                sb.append("<div class='diag-info'>")
                  .append("<span>PageRank: ")
                  .append(String.format("%.6f", item.getPageRank()))
                  .append("</span>")
                  .append("<span> | TF-IDF: ")
                  .append(String.format("%.6f", item.getTfIdf()))
                  .append("</span>")
                  .append("<span> | <b>Final Score (norm): ")
                  .append(String.format("%.4f", item.getFinalScore()))
                  .append("</b></span>")
                  .append("<span> | Geo: ");

                if (geoOn) {
                    sb.append("ON (")
                      .append(htmlEscape(geoCity))
                      .append("), boost=")
                      .append(String.format("%.2f", geoBoost));
                } else {
                    sb.append("OFF");
                }

                sb.append("</span>")
                                    .append("</div>");

                                if (item.isTfIdfIncomplete()) {
                                    double percent = item.getTfIdfScale() * 100.0;
                                    if (percent < 0.0) {
                                        percent = 0.0;
                                    } else if (percent > 100.0) {
                                        percent = 100.0;
                                    }
                                    sb.append("<div class='diag-note'>TF-IDF job still building; scores scaled by ")
                                      .append(String.format("%.1f%%", percent))
                                      .append(".</div>");
                                }

                                List<TfIdfComponent> breakdown = item.getTfIdfDetails();
                                if (breakdown != null && !breakdown.isEmpty()) {
                                        sb.append("<div class='diag-tfidf-breakdown'>Top TF-IDF terms: ");
                                        int limit = Math.min(5, breakdown.size());
                                        for (int i = 0; i < limit; i++) {
                                                TfIdfComponent component = breakdown.get(i);
                                                sb.append(htmlEscape(component.getWord()))
                                                    .append("=")
                                                    .append(String.format("%.4f", component.getTfidf()));
                                                if (i < limit - 1) {
                                                        sb.append(", ");
                                                }
                                        }
                                        sb.append("</div>");
                                }
            }

            sb.append("</div>");
        }

        return sb.toString();
    }


    private static String buildDidYouMeanHtml(String didYouMean) {
        if (didYouMean == null) return "";
        String trimmed = didYouMean.trim();
        if (trimmed.isEmpty()) return "";
        try {
            String link = "/search?query=" +
                    URLEncoder.encode(trimmed, StandardCharsets.UTF_8);
            return "<div class='didyoumean'>Did you mean " +
                    "<a href='" + link + "'>" + htmlEscape(trimmed) +
                    "</a>?</div>";
        } catch (Exception e) {
            return "";
        }
    }

    private static String htmlEscape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            switch (c) {
                case '&': sb.append("&amp;"); break;
                case '<': sb.append("&lt;"); break;
                case '>': sb.append("&gt;"); break;
                case '"': sb.append("&quot;"); break;
                case '\'': sb.append("&#39;"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String htmlAttributeEscape(String s) {
        // Additional escaping for attribute values
        return htmlEscape(s).replace("\"", "&quot;");
    }
}
