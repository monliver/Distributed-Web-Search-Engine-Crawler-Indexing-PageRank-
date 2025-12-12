package frontend.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import frontend.storage.KVSStorage;

/** Spellchecker and autocomplete backed by index words. */
public class SpellChecker {

    private static final int MAX_EDIT_DISTANCE      = 2;
    private static final int MAX_SPELL_CANDIDATES   = 1; // best match per token

    private static volatile List<String> DICTIONARY = null;

    /** Load dictionary exactly once. */
    private static synchronized void ensureDictionaryLoaded() {
        if (DICTIONARY != null) {
            return;
        }
        List<String> words = KVSStorage.getAllIndexWords();
        // Deduplicate + sort for stable suggestions
        Set<String> set = new HashSet<>();
        ArrayList<String> unique = new ArrayList<>();
        for (String w : words) {
            if (w == null) continue;
            String lw = w.toLowerCase(Locale.ROOT);
            if (set.add(lw)) {
                unique.add(lw);
            }
        }
        Collections.sort(unique);
        DICTIONARY = unique;
        System.out.println("[SpellChecker] Dictionary loaded, size = " + DICTIONARY.size());
    }

    /** Suggest corrected query when tokens misspell dictionary words. */
    public static String suggestQuery(List<String> tokens) {
        ensureDictionaryLoaded();

        if (tokens == null || tokens.isEmpty() || DICTIONARY.isEmpty()) {
            return null;
        }

        List<String> corrected = new ArrayList<>();
        boolean changed = false;

        for (String token : tokens) {
            if (token == null || token.isEmpty()) {
                continue;
            }
            String lower = token.toLowerCase(Locale.ROOT);

            // If token already exists as a dictionary word, keep it
            if (DICTIONARY.contains(lower)) {
                corrected.add(lower);
                continue;
            }

            String best = null;
            int bestDist = Integer.MAX_VALUE;

            for (String cand : DICTIONARY) {
                // Quick length heuristic to skip far-away candidates
                if (Math.abs(cand.length() - lower.length()) > MAX_EDIT_DISTANCE) {
                    continue;
                }
                int dist = editDistance(lower, cand, bestDist);
                if (dist < bestDist && dist <= MAX_EDIT_DISTANCE) {
                    bestDist = dist;
                    best = cand;
                    // We only need the single best candidate
                    if (bestDist == 1 && MAX_SPELL_CANDIDATES == 1) {
                        break;
                    }
                }
            }

            if (best != null) {
                corrected.add(best);
                changed = true;
            } else {
                corrected.add(lower);
            }
        }

        if (!changed) {
            return null;
        }
        return String.join(" ", corrected);
    }

    /** Return up to {@code limit} autocomplete candidates for a prefix. */
    public static List<String> suggestPrefix(String prefix, int limit) {
        ensureDictionaryLoaded();

        List<String> out = new ArrayList<>();
        if (prefix == null || prefix.isEmpty() ||
            DICTIONARY.isEmpty() || limit <= 0) {
            return out;
        }

        String p = prefix.toLowerCase(Locale.ROOT);

        for (String w : DICTIONARY) {
            if (w.startsWith(p)) {
                out.add(w);
                if (out.size() >= limit) {
                    break;
                }
            }
        }
        return out;
    }

    /** Compute Levenshtein distance with early exit beyond currentBest. */
    private static int editDistance(String a, String b, int currentBest) {
        int n = a.length();
        int m = b.length();

        if (n == 0) return m;
        if (m == 0) return n;

        // If length difference already exceeds currentBest, skip
        if (Math.abs(n - m) > currentBest) {
            return currentBest + 1;
        }

        int[] prev = new int[m + 1];
        int[] cur  = new int[m + 1];

        for (int j = 0; j <= m; j++) {
            prev[j] = j;
        }

        for (int i = 1; i <= n; i++) {
            cur[0] = i;
            char ca = a.charAt(i - 1);

            int rowMin = cur[0];

            for (int j = 1; j <= m; j++) {
                char cb = b.charAt(j - 1);
                int cost = (ca == cb) ? 0 : 1;

                int del = prev[j] + 1;
                int ins = cur[j - 1] + 1;
                int sub = prev[j - 1] + cost;

                int v = Math.min(del, Math.min(ins, sub));
                cur[j] = v;

                if (v < rowMin) {
                    rowMin = v;
                }
            }

            // Early exit if even best in this row already exceeds currentBest
            if (rowMin > currentBest) {
                return rowMin;
            }

            int[] tmp = prev;
            prev = cur;
            cur = tmp;
        }

        return prev[m];
    }
}
