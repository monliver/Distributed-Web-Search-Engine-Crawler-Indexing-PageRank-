package frontend.view;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class FileUtil {

    // Reads static content from classpath or disk.
    public static String readFromFile(String path) {
        if (path == null || path.isEmpty()) {
            return "Error reading file: <empty path>";
        }

        // Normalize path: remove leading slash and unify separators
        String normalized = path.replace("\\", "/");
        if (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }

        // try these candidate paths in order
        String[] candidates = new String[] {
                normalized,
                "frontend/static/" + normalized   // in case caller passes only "homePage.html"
        };

        // 1) Try from CLASSPATH
        for (String candidate : candidates) {
            try {
                InputStream in = FileUtil.class
                        .getClassLoader()
                        .getResourceAsStream(candidate);
                if (in != null) {
                    System.out.println("[FileUtil] Loaded from CLASSPATH: " + candidate);
                    try (BufferedReader br = new BufferedReader(
                            new InputStreamReader(in, StandardCharsets.UTF_8))) {
                        return br.lines().collect(Collectors.joining("\n"));
                    }
                }
            } catch (Exception e) {
                // Keep going and try the next candidate
                System.out.println("[FileUtil] Failed CLASSPATH load for "
                        + candidate + " : " + e.getMessage());
            }
        }

        // 2) Try from local file system
        for (String candidate : candidates) {
            try {
                Path p = Paths.get(candidate);
                System.out.println("[FileUtil] Try local path = " + p.toAbsolutePath());
                if (Files.exists(p)) {
                    System.out.println("[FileUtil] Loaded from FILE: " + p.toAbsolutePath());
                    return Files.readString(p, StandardCharsets.UTF_8);
                } else {
                    System.out.println("[FileUtil] File does not exist: " + p.toAbsolutePath());
                }
            } catch (Exception e) {
                System.out.println("[FileUtil] Failed FILE load for "
                        + candidate + " : " + e.getMessage());
            }
        }

        // If everything failed, return a clear error message
        return "Error reading file: " + path + " (no candidate path worked)";
    }
}
