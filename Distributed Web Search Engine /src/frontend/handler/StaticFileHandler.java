package frontend.handler;

import frontend.view.FileUtil;
import webserver.Request;
import webserver.Response;
import webserver.Route;

public class StaticFileHandler implements Route {

    @Override
    public Object handle(Request req, Response res) throws Exception {
        String file = req.params("file");
        String content = FileUtil.readFromFile("frontend/static/" + file);

        if (file.endsWith(".css")) res.type("text/css"); 
        else if (file.endsWith(".html")) res.type("text/html"); 
        else if (file.endsWith(".js")) res.type("application/javascript");

        return content;
    }
}

