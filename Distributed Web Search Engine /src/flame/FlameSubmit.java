package flame;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;

public class FlameSubmit {

    static int responseCode;
    static String errorResponse;

    public static String submit(String server, String jarFileName, String className, String arg[]) throws Exception {
        responseCode = 200;
        errorResponse = null;
        String u = "http://" + server + "/submit" + "?class=" + className;
        for (int i = 0; i < arg.length; i++) {
            u = u + "&arg" + (i + 1) + "=" + URLEncoder.encode(arg[i], "UTF-8");
        }

        File f = new File(jarFileName);
        byte jarFile[] = new byte[(int) f.length()];
        FileInputStream fis = new FileInputStream(jarFileName);
        fis.read(jarFile);
        fis.close();

        HttpURLConnection con = (HttpURLConnection) (new URI(u).toURL()).openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setFixedLengthStreamingMode(jarFile.length);
        con.setRequestProperty("Content-Type", "application/jar-archive");
        con.connect();
        OutputStream out = con.getOutputStream();
        out.write(jarFile);

        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String result = "";
            while (true) {
                String s = r.readLine();
                if (s == null) {
                    break;
                }
                // Stream live progress
                System.out.println(s);
                System.out.flush();
                result = result + (result.equals("") ? "" : "\n") + s;
            }

            return result;
        } catch (IOException ioe) {
            responseCode = con.getResponseCode();
            BufferedReader r = new BufferedReader(new InputStreamReader(con.getErrorStream()));
            errorResponse = "";
            while (true) {
                String s = r.readLine();
                if (s == null) {
                    break;
                }

                errorResponse = errorResponse + (errorResponse.equals("") ? "" : "\n") + s;
            }

            return errorResponse;
        }
    }

    public static int getResponseCode() {
        return responseCode;
    }

    public static String getErrorResponse() {
        return errorResponse;
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 3) {
            System.err.println("Syntax: FlameSubmit <server> <jarFile> <className> [args...]");
            System.exit(1);
        }

        String[] arg = new String[args.length - 3];
        for (int i = 3; i < args.length; i++) {
            arg[i - 3] = args[i];
        }

        try {
            String response = submit(args[0], args[1], args[2], arg);
            // Response already streamed
            if (response == null) {
                System.err.println("*** JOB FAILED ***\n");
                System.err.println(getErrorResponse());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
