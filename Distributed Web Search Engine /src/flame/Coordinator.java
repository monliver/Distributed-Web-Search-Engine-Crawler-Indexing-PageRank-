package flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.reflect.*;

import static webserver.Server.*;
import kvs.KVSClient;
import tools.*;

class Coordinator extends generic.Coordinator {

    private static final Logger logger = Logger.getLogger(Coordinator.class);
    private static final String version = "v1.5";

    static int nextJobID = 1;
    public static KVSClient kvs;

    public static void main(String args[]) {

        // Check the command-line arguments
        if (args.length != 2) {
            System.err.println("Syntax: Coordinator <port> <kvsCoordinator>");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        kvs = new KVSClient(args[1]);

        logger.info("Flame coordinator (" + version + ") starting on port " + myPort);

        port(myPort);
        registerRoutes();

        /* Serve worker list page */
        get("/", (request, response) -> {
            response.type("text/html");
            return "<html><head><title>Flame coordinator</title></head><body><h3>Flame Coordinator</h3>\n" + clientTable() + "</body></html>";
        });

        /* Accept job submissions from FlameSubmit */
        post("/submit", (request, response) -> {

            // Parse class + arg query string
            String className = request.queryParams("class");
            logger.info("New job submitted; main class is " + className);

            if (className == null) {
                response.status(400, "Bad request");
                return "Missing class name (parameter 'class')";
            }

            Vector<String> argVector = new Vector<String>();
            for (int i = 1; request.queryParams("arg" + i) != null; i++) {
                argVector.add(URLDecoder.decode(request.queryParams("arg" + i), "UTF-8"));
            }

            // Upload JAR to all workers in parallel
            Thread threads[] = new Thread[getWorkers().size()];
            String results[] = new String[getWorkers().size()];
            for (int i = 0; i < getWorkers().size(); i++) {
                final String url = "http://" + getWorkers().elementAt(i) + "/useJAR";
                final int j = i;
                threads[i] = new Thread("JAR upload #" + (i + 1)) {
                    public void run() {
                        try {
                            results[j] = new String(HTTP.doRequest("POST", url, request.bodyAsBytes()).body());
                        } catch (Exception e) {
                            results[j] = "Exception: " + e;
                            e.printStackTrace();
                        }
                    }
                };
                threads[i].start();
            }

            // Wait for uploads to finish
            for (int i = 0; i < threads.length; i++) {
                try {
                    threads[i].join();
                } catch (InterruptedException ie) {
                }
            }

            // Cache the submitted JAR locally for reflection
            int id = nextJobID++;
            String jarName = "job-" + id + ".jar";
            File jarFile = new File(jarName);
            FileOutputStream fos = new FileOutputStream(jarFile);
            fos.write(request.bodyAsBytes());
            fos.close();

            // Invoke job run(context,args) via reflection
            FlameContextImpl context = new FlameContextImpl(jarName);
            try {
                Loader.invokeRunMethod(jarFile, className, context, argVector);
            } catch (IllegalAccessException iae) {
                response.status(400, "Bad request");
                return "Double-check that the class " + className + " contains a public static run(FlameContext, String[]) method, and that the class itself is public!";
            } catch (NoSuchMethodException iae) {
                response.status(400, "Bad request");
                return "Double-check that the class " + className + " contains a public static run(FlameContext, String[]) method";
            } catch (InvocationTargetException ite) {
                logger.error("The job threw an exception, which was:", ite.getCause());
                StringWriter sw = new StringWriter();
                ite.getCause().printStackTrace(new PrintWriter(sw));
                response.status(500, "Job threw an exception");
                return sw.toString();
            }

            return context.getOutput();
        });

        get("/version", (request, response) -> {
            return version;
        });
    }
}
