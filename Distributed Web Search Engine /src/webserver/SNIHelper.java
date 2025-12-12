package webserver;

import tools.SNIInspector;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.util.*;

/**
 * Extra Credit: SNI-based multi-host HTTPS support.
 *
 * Usage:
 *   SNIHelper.addKeystore("example.com", "keystore_example.jks", "secret");
 *   ServerSocket sss = SNIHelper.createSSLServerSocket(443);
 */
public class SNIHelper {

    // host -> SSLContext
    private static final Map<String, SSLContext> contexts = new HashMap<>();

    /**
     * Register a new hostname -> keystore
     */
    public static void addKeystore(String hostname, String keystoreFile, String password) throws Exception {
        char[] pass = password.toCharArray();

        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(keystoreFile)) {
            ks.load(fis, pass);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, pass);

        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(kmf.getKeyManagers(), null, null);

        contexts.put(hostname.toLowerCase(Locale.US), sc);
    }

    /**
     * Create a special ServerSocket that inspects SNI and selects the right context.
     * It binds explicitly to IPv4 (0.0.0.0) to avoid IPv6 issues on AWS.
     */
    public static ServerSocket createSSLServerSocket(int port) throws Exception {
        ServerSocket plain = new ServerSocket();
        plain.bind(new InetSocketAddress("0.0.0.0", port));

        return new ServerSocket() {
            @Override
            public Socket accept() throws IOException {
                Socket sock = plain.accept();
                try {
                    // Inspect first bytes for SNI
                    SNIInspector inspector = new SNIInspector();
                    inspector.parseConnection(sock);

                    String hostname = null;
                    if (inspector.getHostName() != null) {
                        hostname = inspector.getHostName().getAsciiName().toLowerCase(Locale.US);
                    }

                    SSLContext ctx = contexts.get(hostname);
                    if (ctx == null && !contexts.isEmpty()) {
                        ctx = contexts.values().iterator().next(); // fallback
                    }
                    if (ctx == null) {
                        throw new IOException("No SSLContext available for hostname " + hostname);
                    }

                    // Wrap the socket with SSL
                    SSLSocketFactory sf = ctx.getSocketFactory();
                    return sf.createSocket(
                            sock,
                            sock.getInetAddress().getHostAddress(),
                            sock.getPort(),
                            true
                    );

                } catch (Exception e) {
                    throw new IOException("SNI negotiation failed", e);
                }
            }

            @Override
            public void close() throws IOException {
                plain.close();
            }

            @Override
            public boolean isClosed() {
                return plain.isClosed();
            }
        };
    }
}
