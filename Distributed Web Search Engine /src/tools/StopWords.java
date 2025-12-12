package tools;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * English stopword list (based on NLTK standard stopwords).
 *
 * For the base list:
 *   terminal:
 *     pip install nltk
 *     python3
 *       >>> import nltk
 *       >>> from nltk.corpus import stopwords
 *       >>> nltk.download('stopwords')
 *
 * We keep everything in a HashSet for O(1) lookups.
 *
 * Additionally, we hard-code some very frequent web-specific tokens
 * (such as "http", "www", "com", "html", "login", "cookie", etc.)
 * to reduce the size of the TF-IDF/dictionary tables and speed up
 * the PageRank+TF-IDF job on large crawls (e.g., 1M pages).
 */
public class StopWords {

    public static final Set<String> STOP_WORD_SET = new HashSet<>(Arrays.asList(
        // --- Standard English stop words (NLTK style) ---
        "a", "about", "above", "after", "again", "against", "ain", "all", "am", "an",
        "and", "any", "are", "aren", "aren't", "as", "at", "be", "because", "been",
        "before", "being", "below", "between", "both", "but", "by", "can", "couldn", "couldn't",
        "d", "did", "didn", "didn't", "do", "does", "doesn", "doesn't", "doing", "don",
        "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn",
        "hadn't", "has", "hasn", "hasn't", "have", "haven", "haven't", "having", "he", "he'd",
        "he'll", "her", "here", "hers", "herself", "he's", "him", "himself", "his", "how",
        "i", "i'd", "if", "i'll", "i'm", "in", "into", "is", "isn", "isn't",
        "it", "it'd", "it'll", "it's", "its", "itself", "i've", "just", "ll", "m",
        "ma", "me", "mightn", "mightn't", "more", "most", "mustn", "mustn't", "my", "myself",
        "needn", "needn't", "no", "nor", "not", "now", "o", "of", "off", "on",
        "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own",
        "re", "s", "same", "shan", "shan't", "she", "she'd", "she'll", "she's", "should",
        "shouldn", "shouldn't", "should've", "so", "some", "such", "t", "than", "that", "that'll",
        "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "they'd",
        "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
        "up", "ve", "very", "was", "wasn", "wasn't", "we", "we'd", "we'll", "we're",
        "were", "weren", "weren't", "we've", "what", "when", "where", "which", "while", "who",
        "whom", "why", "will", "with", "won", "won't", "wouldn", "wouldn't", "y", "you",
        "you'd", "you'll", "your", "you're", "yours", "yourself", "yourselves", "you've",

        // --- Extra web-specific high-frequency tokens (hardcoded) ---
        // Common protocol / domain fragments
        "http", "https", "www", "com", "org", "net",
        // File / markup / tech terms that appear everywhere
        "html", "htm", "css", "js", "javascript", "script", "href", "src", "img",
        "json", "xml", "utf", "utf8", "iso",
        // Generic site words that don't help ranking much
        "page", "pages", "site", "sites", "homepage", "home", "index",
        "nav", "menu", "footer", "header", "sidebar",
        // Auth / account noise
        "login", "logout", "signin", "signup", "register", "account",
        // Policy / legal boilerplate
        "cookie", "cookies", "policy", "policies", "privacy", "terms",
        "agreement", "copyright", "trademark",
        // Misc. UI / layout words
        "click", "button", "submit", "search", "results", "filter"
    ));

    public static boolean isStopWord(String word) {
        if (word == null) return false;
        return STOP_WORD_SET.contains(word.toLowerCase());
    }
}
