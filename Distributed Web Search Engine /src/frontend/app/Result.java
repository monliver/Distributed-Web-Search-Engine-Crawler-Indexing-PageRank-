package frontend.app;

import java.util.Collections;
import java.util.List;

import frontend.storage.KVSStorage.TfIdfComponent;

// Holds ranking data for a search hit.
public class Result {
    private String url;
    private String title;
    private String snippet;
    private double pageRank;
    private double tfIdf;
    private double finalScore;
    private List<TfIdfComponent> tfIdfDetails;
    private boolean tfIdfIncomplete;
    private double tfIdfScale;

    // Geo-related fields
    private double geoBoost;    // 0 or 1
    private String geoCity;     // city name

    // Builds a result without a final score.
    public Result(String url,
                  String title,
                  String snippet,
                  double pageRank,
                  double tfIdf)  {
        this(url, title, snippet, pageRank, tfIdf, 0.0);
    }

    // Builds a result with a supplied final score.
    public Result(String url,
                  String title,
                  String snippet,
                  double pageRank,
                  double tfIdf,
                  double finalScore) {
        this.url = url;
        this.title = title;
        this.snippet = snippet;
        this.pageRank = pageRank;
        this.tfIdf = tfIdf;
        this.finalScore = finalScore;

        this.geoBoost = 0.0;
        this.geoCity = null;
        this.tfIdfDetails = Collections.emptyList();
        this.tfIdfIncomplete = false;
        this.tfIdfScale = 1.0;
    }

    // Basic getters / setters

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSnippet() {
        return snippet;
    }

    public void setSnippet(String snippet) {
        this.snippet = snippet;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public double getTfIdf() {
        return tfIdf;
    }

    public void setTfIdf(double tfIdf) {
        this.tfIdf = tfIdf;
    }

    public double getFinalScore() {
        return finalScore;
    }

    public void setFinalScore(double finalScore) {
        this.finalScore = finalScore;
    }

    public List<TfIdfComponent> getTfIdfDetails() {
        return tfIdfDetails;
    }

    public void setTfIdfDetails(List<TfIdfComponent> tfIdfDetails) {
        this.tfIdfDetails = (tfIdfDetails == null) ? Collections.emptyList() : tfIdfDetails;
    }

    public boolean isTfIdfIncomplete() {
        return tfIdfIncomplete;
    }

    public void setTfIdfIncomplete(boolean tfIdfIncomplete) {
        this.tfIdfIncomplete = tfIdfIncomplete;
    }

    public double getTfIdfScale() {
        return tfIdfScale;
    }

    public void setTfIdfScale(double tfIdfScale) {
        this.tfIdfScale = tfIdfScale;
    }

    // Geo accessors

    public double getGeoBoost() {
        return geoBoost;
    }

    public void setGeoBoost(double geoBoost) {
        this.geoBoost = geoBoost;
    }

    public String getGeoCity() {
        return geoCity;
    }

    public void setGeoCity(String geoCity) {
        this.geoCity = geoCity;
    }

    // Sets geo fields together.
    public void setGeoInfo(double geoBoost, String geoCity) {
        this.geoBoost = geoBoost;
        this.geoCity = geoCity;
    }
}
