package jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Seeds and blacklist presets. */
public final class Urls {
    public static final String[] DEFAULT_SEEDS = new String[] {
            "https://en.wikipedia.org/wiki/Main_Page",
            "https://www.britannica.com/",
            "https://www.nationalgeographic.com/",
            "https://www.reuters.com/",
            "https://www.bbc.com/news",
            "https://www.npr.org/",
            "https://www.upenn.edu/",
            "https://www.stanford.edu/",
            "https://www.mit.edu/",
            "https://www.harvard.edu/",
            "https://www.nasa.gov/",
            "https://www.cdc.gov/",
            "https://www.noaa.gov/",
            "https://developer.mozilla.org/",
            "https://www.wikihow.com/",
            "https://www.history.com/",
            "https://www.howstuffworks.com/",
            "https://www.sciencedaily.com/",
            "https://www.poetryfoundation.org/",
            "https://www.goodreads.com/",
            "https://www.khanacademy.org/",
            "https://www.ted.com/",
            "https://www.quora.com/",
            "https://www.apnews.com/",
            "https://www.aljazeera.com/",
            "https://www.theguardian.com/",
            "https://www.nytimes.com/",
            "https://www.economist.com/",
            "https://www.washingtonpost.com/",
            "https://www.latimes.com/",
            "https://www.wsj.com/",
            "https://www.berkeley.edu/",
            "https://www.cornell.edu/",
            "https://www.yale.edu/",
            "https://www.princeton.edu/",
            "https://www.columbia.edu/",
            "https://www.caltech.edu/",
            "https://www.cam.ac.uk/",
            "https://www.ox.ac.uk/",
            "https://arxiv.org/",
            "https://www.science.org/",
            "https://www.scientificamerican.com/",
            "https://www.smithsonianmag.com/",
            "https://www.space.com/",
            "https://www.nature.com/",
            "https://www.acm.org/",
            "https://ieeexplore.ieee.org/",
            "https://stackoverflow.com/",
            "https://www.github.com/",
            "https://www.geeksforgeeks.org/",
            "https://www.w3schools.com/",
            "https://www.python.org/",
            "https://www.oracle.com/java/",
            "https://kubernetes.io/docs/",
            "https://www.linux.org/",
            "https://www.gnu.org/",
            "https://www.metmuseum.org/",
            "https://www.brookings.edu/",
            "https://www.archives.gov/",
            "https://www.loc.gov/",
            "https://www.usa.gov/",
            "https://www.whitehouse.gov/",
            "https://www.federalreserve.gov/",
            "https://www.sec.gov/",
            "https://www.energy.gov/",
            "https://www.nih.gov/",
            "https://www.lonelyplanet.com/",
            "https://www.travelandleisure.com/",
            "https://www.atlasobscura.com/",
            "https://www.fandom.com/",
            "https://www.imdb.com/",
            "https://www.gutenberg.org/",
            "https://www.stackexchange.com/",
            "https://www.openstreetmap.org/",
            "https://www.apple.com/",
            "https://www.healthline.com/",
    };

    // Host blacklist categories
    public static final Set<String> SOCIAL_MEDIA_HOSTS = unmodifiableSet(
            "facebook.com", "www.facebook.com",
            "twitter.com", "www.twitter.com",
            "t.co",
            "instagram.com", "www.instagram.com",
            "linkedin.com", "www.linkedin.com",
            "youtube.com", "www.youtube.com",
            "tiktok.com", "www.tiktok.com",
            "reddit.com", "www.reddit.com",
            "quora.com", "www.quora.com",
            "medium.com", "www.medium.com",
            "pinterest.com", "www.pinterest.com"
    );

    public static final Set<String> TRANSLATION_AND_SEARCH_HOSTS = unmodifiableSet(
            "m.baidu.com", "baidu.com",
            "translate.google.com"
    );

    public static final Set<String> TRACKING_AND_AD_HOSTS = unmodifiableSet(
            "doubleclick.net", "googletagmanager.com",
            "google-analytics.com", "adservice.google.com",
            "scorecardresearch.com",
            "taboola.com", "outbrain.com"
    );

    public static final Set<String> CDN_AND_STATIC_HOSTS = unmodifiableSet(
            "api.nytimes.com", "api.bbc.com",
            "static.nytimes.com",
            "cdn.cnn.com", "cdn.britannica.com",
            "akamaihd.net", "cloudfront.net",
            "fastly.net"
    );

    public static final Set<String> LOCAL_HOSTS = unmodifiableSet(
            "localhost", "127.0.0.1"
    );

    public static final Set<String> BLACKLISTED_HOSTS;

    // URL substring blacklist categories
    public static final List<String> TRACKING_QUERY_SUBSTRINGS = List.of(
            "?utm_", "&utm_", "gclid=", "fbclid="
    );

    public static final List<String> REFERRAL_QUERY_SUBSTRINGS = List.of(
            "?ref=", "?source="
    );

    public static final List<String> AUTH_PATH_SUBSTRINGS = List.of(
            "/login", "/signin", "/logout",
            "/subscribe", "/account", "/profile",
            "/register", "/checkout", "/cart"
    );

    public static final List<String> SEARCH_PATH_SUBSTRINGS = List.of(
            "/search?",
            "/?q=",
            "query="
    );

    public static final List<String> CALENDAR_PATH_SUBSTRINGS = List.of(
            "/calendar", "/events/", "/year/", "/month/", "/day/"
    );

    public static final List<String> SOCIAL_SHARE_SUBSTRINGS = List.of(
            "/share?", "/facebook.com/sharer", "/twitter.com/share"
    );

    public static final List<String> API_AND_FEED_SUBSTRINGS = List.of(
            "/api/", "/graphql", "/ajax/", "/feeds/",
            ".json", ".xml"
    );

    public static final List<String> PRINT_VIEW_SUBSTRINGS = List.of(
            "/print/", "?print"
    );

    public static final List<String> SESSION_SUBSTRINGS = List.of(
            ";jsessionid="
    );

    public static final List<String> BLACKLISTED_SUBSTRINGS;

    // Extension blacklist categories
    public static final List<String> IMAGE_EXTENSIONS = List.of(
            ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp"
    );

    public static final List<String> VIDEO_AUDIO_EXTENSIONS = List.of(
            ".mp4", ".mov", ".avi", ".mkv", ".webm", ".mp3", ".wav"
    );

    public static final List<String> DOCUMENT_ARCHIVE_EXTENSIONS = List.of(
            ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
            ".zip", ".tar", ".gz", ".tgz", ".rar", ".7z"
    );

    public static final List<String> FONT_STYLE_EXTENSIONS = List.of(
            ".css", ".js", ".woff", ".woff2", ".ttf", ".eot"
    );

    public static final List<String> ERROR_PAGE_EXTENSIONS = List.of(
            ".php", ".aspx"
    );

    public static final List<String> BLACKLISTED_EXTENSIONS;

    public static final List<String> BLACKLISTED_LANGUAGE = List.of(
            "/es/", "/de/", "/fr/", "/ja/", "/zh/", "/ru/", "/ar/"
    );

    static {
        Set<String> hosts = new LinkedHashSet<>();
        hosts.addAll(SOCIAL_MEDIA_HOSTS);
        hosts.addAll(TRANSLATION_AND_SEARCH_HOSTS);
        hosts.addAll(TRACKING_AND_AD_HOSTS);
        hosts.addAll(CDN_AND_STATIC_HOSTS);
        hosts.addAll(LOCAL_HOSTS);
        BLACKLISTED_HOSTS = Collections.unmodifiableSet(hosts);

        List<String> substrings = new ArrayList<>();
        substrings.addAll(TRACKING_QUERY_SUBSTRINGS);
        substrings.addAll(REFERRAL_QUERY_SUBSTRINGS);
        substrings.addAll(AUTH_PATH_SUBSTRINGS);
        substrings.addAll(SEARCH_PATH_SUBSTRINGS);
        substrings.addAll(CALENDAR_PATH_SUBSTRINGS);
        substrings.addAll(SOCIAL_SHARE_SUBSTRINGS);
        substrings.addAll(API_AND_FEED_SUBSTRINGS);
        substrings.addAll(PRINT_VIEW_SUBSTRINGS);
        substrings.addAll(SESSION_SUBSTRINGS);
        BLACKLISTED_SUBSTRINGS = Collections.unmodifiableList(substrings);

        List<String> extensions = new ArrayList<>();
        extensions.addAll(IMAGE_EXTENSIONS);
        extensions.addAll(VIDEO_AUDIO_EXTENSIONS);
        extensions.addAll(DOCUMENT_ARCHIVE_EXTENSIONS);
        extensions.addAll(FONT_STYLE_EXTENSIONS);
        extensions.addAll(ERROR_PAGE_EXTENSIONS);
        BLACKLISTED_EXTENSIONS = Collections.unmodifiableList(extensions);
    }

    private Urls() {
        // Utility class
    }

    private static Set<String> unmodifiableSet(String... values) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(values)));
    }
}
