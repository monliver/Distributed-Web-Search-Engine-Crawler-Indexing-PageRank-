package kvs;

import webserver.Server;

public class Coordinator extends generic.Coordinator {
    
    public static void main(String[] args) {
        // Expect single port arg
        if (args.length != 1) {
            System.err.println("Usage: java kvs.Coordinator <port>");
            System.exit(1);
        }
        
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: Port must be a valid integer");
            System.exit(1);
            return;
        }
        
        // Configure server port
        Server.port(port);
        
        // Hook generic routes
        registerRoutes();
        
        // Serve coordinator dashboard
        Server.get("/", (request, response) -> {
            response.type("text/html");
            String html = "<!DOCTYPE html>" +
                         "<html>" +
                         "<head><title>KVS Coordinator</title></head>" +
                         "<body>" +
                         "<h1>KVS Coordinator</h1>" +
                         workerTable() +
                         "</body>" +
                         "</html>";
            return html;
        });
        
        System.out.println("KVS Coordinator started on port " + port);
    }
}