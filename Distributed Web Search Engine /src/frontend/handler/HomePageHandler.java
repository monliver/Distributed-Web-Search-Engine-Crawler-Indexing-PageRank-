package frontend.handler;

import frontend.view.FileUtil;
import webserver.Request;
import webserver.Response;
import webserver.Route;

public class HomePageHandler implements Route {
    @Override
    public Object handle(Request req, Response res) throws Exception {

        // response html
        res.type("text/html");

        return FileUtil.readFromFile("frontend/static/homePage.html");
    }
}
