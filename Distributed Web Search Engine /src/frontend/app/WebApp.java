package frontend.app;

import frontend.handler.CachePageHandler;
import frontend.handler.HomePageHandler;
import frontend.handler.ResultPageHandler;
import frontend.handler.StaticFileHandler;
import frontend.handler.SuggestHandler;
import static webserver.Server.get;
import static webserver.Server.securePort;

// Bootstraps the frontend HTTP routes.
public class WebApp {

    public static void main(String[] args) {

        // Optional HTTP port (default 8080)
        int httpPort = 8080;
        if (args.length > 0) {
            try {
                httpPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.err.println("Usage: java frontend.app.WebApp [httpPort]");
                System.exit(1);
            }
        }

        webserver.Server.port(httpPort);
        securePort(443);

        // Register handlers
        get("/",      new HomePageHandler());
        get("/search", new ResultPageHandler());
        get("/cache",  new CachePageHandler());
        get("/suggest", new SuggestHandler());

        // Static assets
        get("/static/:file", new StaticFileHandler());

        System.out.println("MegaSearch server running on:");
        System.out.println("  HTTP:  http://localhost:" + httpPort);
    }
}
