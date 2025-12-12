package frontend.handler;

import java.util.List;

import frontend.tools.SpellChecker;
import webserver.Request;
import webserver.Response;
import webserver.Route;

// Returns autocomplete suggestions as JSON.
public class SuggestHandler implements Route {

    private static final int SUGGESTION_LIMIT = 8;

    @Override
    public Object handle(Request req, Response res) throws Exception {
        String prefix = req.queryParams("prefix");
        if (prefix == null) prefix = "";

        List<String> suggestions = SpellChecker.suggestPrefix(prefix, SUGGESTION_LIMIT);

        res.type("application/json; charset=UTF-8");

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i = 0; i < suggestions.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"")
              .append(escapeJson(suggestions.get(i)))
              .append("\"");
        }

        sb.append("]");
        return sb.toString();
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"':  sb.append("\\\""); break;
                case '\b': sb.append("\\b");  break;
                case '\f': sb.append("\\f");  break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
