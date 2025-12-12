package frontend.handler;

import frontend.storage.KVSStorage;
import webserver.Request;
import webserver.Response;
import webserver.Route;

// Serves cached HTML from pt-crawl.
public class CachePageHandler implements Route {

    @Override
    public Object handle(Request req, Response res) throws Exception {
        String url = req.queryParams("url");
        if (url == null || url.isEmpty()) {
            res.status(400, "Bad Request");
            return "Missing 'url' query parameter";
        }

        String html = KVSStorage.getCachedPage(url);
        if (html == null) {
            res.status(404, "Not Found");
            return "No cached copy for URL: " + url;
        }

        // Always reply with HTML
        res.type("text/html; charset=UTF-8");
        return html;
    }
}
