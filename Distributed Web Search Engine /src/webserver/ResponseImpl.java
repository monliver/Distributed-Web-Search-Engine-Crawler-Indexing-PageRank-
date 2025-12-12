package webserver;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Response implementation for HW3.
 * Handles status, headers, body, commit, halt, and write().
 */
public class ResponseImpl implements Response {
    private int statusCode = 200;
    private String statusMessage = "OK";
    private final Map<String, String> headers = new LinkedHashMap<>();
    private byte[] bodyBytes = new byte[0];

    private boolean committed = false;
    private boolean halted = false;
    private boolean writeMode = false; // if write() was used

    private final OutputStream rawOut;

    public ResponseImpl(OutputStream rawOut) {
        this.rawOut = rawOut;
        header("Server", "MySimpleServer");
    }

    // --- Helpers for Server.java ---
    public boolean headersSent() {
        return committed;
    }

    public boolean isHalted() {
        return halted;
    }

    // --- Body methods ---
    @Override
    public void body(String body) {
        if (writeMode) return; // ignore if write() already used
        if (body == null) {
            bodyBytes = new byte[0];
        } else {
            bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            if (!headers.containsKey("Content-Type")) {
                header("Content-Type", "text/plain; charset=utf-8");
            }
        }
    }

    @Override
    public void bodyAsBytes(byte[] body) {
        if (writeMode) return;
        bodyBytes = (body == null) ? new byte[0] : body;
    }

    // --- Headers ---
    @Override
    public void header(String name, String value) {
        if (writeMode) return;
        // Multiple headers with same name allowed
        if (headers.containsKey(name)) {
            headers.put(name, headers.get(name) + "," + value);
        } else {
            headers.put(name, value);
        }
    }

    @Override
    public void type(String contentType) {
        if (!writeMode) {
            header("Content-Type", contentType);
        }
    }

    // --- Status ---
    @Override
    public void status(int code, String reasonPhrase) {
        if (writeMode) return;
        this.statusCode = code;
        this.statusMessage = reasonPhrase;
    }

    public void status(int code) {
        if (writeMode) return;
        this.statusCode = code;
        this.statusMessage = defaultMessage(code);
    }

    // --- Write mode (bypass buffering) ---
    @Override
    public void write(byte[] b) throws Exception {
        if (b == null) return;
        if (!writeMode) {
            // first call: send headers immediately
            writeMode = true;
            committed = true;

            PrintWriter pw = new PrintWriter(rawOut, false, StandardCharsets.ISO_8859_1);
            pw.print("HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n");
            for (Map.Entry<String, String> e : headers.entrySet()) {
                pw.print(e.getKey() + ": " + e.getValue() + "\r\n");
            }
            pw.print("Connection: close\r\n");
            pw.print("\r\n");
            pw.flush();
        }
        try {
            rawOut.write(b);
            rawOut.flush();
        } catch (java.net.SocketException se) {
            // Client disconnected, silently ignore
        }
    }

    // --- Commit buffered response ---
    public void commit() {
        if (committed || writeMode) return;
        committed = true;

        try {
            if (!headers.containsKey("Content-Length")) {
                header("Content-Length", String.valueOf(bodyBytes.length));
            }

            if (!headers.containsKey("Content-Type")) {
                header("Content-Type", "text/html");
            }

            PrintWriter pw = new PrintWriter(rawOut, false, StandardCharsets.ISO_8859_1);
            pw.print("HTTP/1.1 " + statusCode + " " + statusMessage + "\r\n");
            for (Map.Entry<String, String> e : headers.entrySet()) {
                pw.print(e.getKey() + ": " + e.getValue() + "\r\n");
            }
            pw.print("\r\n");
            pw.flush();

            rawOut.write(bodyBytes);
            rawOut.flush();
        } catch (java.net.SocketException se) {
            // Client disconnected, silently ignore
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --- Halt ---
    @Override
    public void halt(int code, String reason) {
        this.statusCode = code;
        this.statusMessage = reason;
        this.body(reason);
        this.halted = true;
    }

    public void halt() {
        this.halted = true;
    }

    // --- Redirect (extra credit) ---
    @Override
    public void redirect(String url, int responseCode) {
        this.statusCode = responseCode;
        this.statusMessage = defaultMessage(responseCode);
        header("Location", url);
        this.bodyBytes = new byte[0];
        this.halted = true;
    }

    // --- Helper ---
    private static String defaultMessage(int code) {
        switch (code) {
            case 200: return "OK";
            case 302: return "Found";
            case 400: return "Bad Request";
            case 403: return "Forbidden";
            case 404: return "Not Found";
            case 500: return "Internal Server Error";
            default:  return "";
        }
    }
}
