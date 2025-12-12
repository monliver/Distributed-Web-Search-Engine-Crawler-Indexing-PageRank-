package kvs;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReplicationManager {
    private static final List<WorkerInfo> workers = new CopyOnWriteArrayList<WorkerInfo>();
    private static String selfId;
    private static int selfPort;
    private static String coordinator;
    private static volatile boolean started = false;

    private static class WorkerInfo {
        private final String id;
        private final String host;
        private final int port;
        WorkerInfo(String id, String host, int port) {
            this.id = id; this.host = host; this.port = port;
        }
        String id() { return id; }
        String host() { return host; }
        int port() { return port; }
    }

    public static void start(String coordinatorAddress, String workerId, int port) {
        if (started) return;
        started = true;
        coordinator = coordinatorAddress;
        selfId = workerId;
        selfPort = port;
        Thread refresher = new Thread(ReplicationManager::refreshLoop, "ReplRefresh");
        refresher.setDaemon(true);
        refresher.start();
        Thread maint = new Thread(ReplicationManager::maintenanceLoop, "ReplMaintenance");
        maint.setDaemon(true);
        maint.start();
    }

    private static void refreshLoop() {
        while (true) {
            try {
                fetchWorkers();
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                return; 
            } catch (Exception ignored) {}
        }
    }

    private static void fetchWorkers() throws Exception {
        URL u = new URL("http://"+coordinator+"/workers");
        HttpURLConnection c = (HttpURLConnection) u.openConnection();
        c.setConnectTimeout(2000);
        c.setReadTimeout(2000);
        try (var in = c.getInputStream(); var s = new java.util.Scanner(in)) {
            if (!s.hasNextLine()) return; // first line count
            s.nextLine();
            List<WorkerInfo> list = new ArrayList<>();
            while (s.hasNextLine()) {
                String line = s.nextLine().trim();
                if (line.isEmpty()) continue;
                // format: id,ip:port
                int comma = line.indexOf(',');
                if (comma < 0) continue;
                String id = line.substring(0, comma);
                String rest = line.substring(comma+1);
                int colon = rest.lastIndexOf(':');
                if (colon < 0) continue;
                String host = rest.substring(0, colon);
                int p = Integer.parseInt(rest.substring(colon+1));
                list.add(new WorkerInfo(id, host, p));
            }
            workers.clear();
            workers.addAll(list);
        }
    }

    // Simple ring ordering by worker ID lexicographically
    private static List<WorkerInfo> ringOrdered() {
        List<WorkerInfo> copy = new ArrayList<WorkerInfo>(workers);
        java.util.Collections.sort(copy, new Comparator<WorkerInfo>() {
            public int compare(WorkerInfo a, WorkerInfo b) { return a.id().compareTo(b.id()); }
        });
        return copy;
    }

    private static WorkerInfo findSelf(List<WorkerInfo> ordered) {
        for (WorkerInfo w : ordered) if (w.id().equals(selfId)) return w;
        return null;
    }

    // Determine if this worker is primary (simplistic: the smallest ID responsible for all keys). For real consistent hashing you'd hash keys; here we just always replicate outward.
    private static boolean isPrimaryForKey(String key) {
        // Basic heuristic: choose worker whose ID is lexicographically >= key hash prefix, else wrap.
        List<WorkerInfo> ordered = ringOrdered();
        if (ordered.isEmpty()) return true;
        String h = sha1Hex(key).substring(0, 4); // prefix
        for (WorkerInfo w : ordered) {
            if (w.id().compareTo(h) >= 0) {
                return w.id().equals(selfId);
            }
        }
        // wrap to first
        return ordered.get(0).id().equals(selfId);
    }

    public static void maybeReplicate(String table, String rowKey, String column, byte[] data, String rawQuery) {
        if (!isPrimaryForKey(rowKey)) return;
        List<WorkerInfo> ordered = ringOrdered();
        if (ordered.size() < 2) return;
        // find self index
        int idx = -1;
        for (int i=0;i<ordered.size();i++) if (ordered.get(i).id().equals(selfId)) { idx = i; break; }
        if (idx < 0) return;
        WorkerInfo w1 = ordered.get((idx - 1 + ordered.size()) % ordered.size());
        WorkerInfo w2 = ordered.get((idx - 2 + ordered.size()) % ordered.size());
        replicateTo(w1, table, rowKey, column, data, rawQuery);
        replicateTo(w2, table, rowKey, column, data, rawQuery);
    }

    private static void replicateTo(WorkerInfo w, String table, String rowKey, String column, byte[] data, String rawQuery) {
        try {
            StringBuilder qs = new StringBuilder();
            if (rawQuery != null && !rawQuery.isEmpty()) {
                qs.append(rawQuery).append('&');
            }
            qs.append("replicated=true");
            URL u = new URL("http://"+w.host()+":"+w.port()+"/data/"+encode(table)+"/"+encode(rowKey)+"/"+encode(column)+(qs.length()>0?"?"+qs:""));
            HttpURLConnection c = (HttpURLConnection) u.openConnection();
            c.setRequestMethod("PUT");
            c.setDoOutput(true);
            c.setConnectTimeout(2000);
            c.setReadTimeout(2000);
            try (OutputStream os = c.getOutputStream()) { os.write(data); }
            c.getResponseCode(); // fire
        } catch (Exception e) {
            System.err.println("Replication forward failed: "+e.getMessage());
        }
    }

    private static String encode(String s) {
        try { return java.net.URLEncoder.encode(s, "UTF-8"); } catch (Exception e) { return s; }
    }

    // Hashing a row: concatenate key + sorted columns + values
    public static String hashRow(Row r) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(r.key().getBytes());
            List<String> cols = new ArrayList<>(r.columns());
            Collections.sort(cols);
            for (String c : cols) {
                md.update(c.getBytes());
                byte[] v = r.getBytes(c);
                if (v != null) md.update(v);
            }
            byte[] dig = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private static void maintenanceLoop() {
        while (true) {
            try {
                performMaintenance();
                Thread.sleep(30000);
            } catch (InterruptedException e) { return; }
              catch (Exception e) { /* ignore */ }
        }
    }

    private static void performMaintenance() {
        List<WorkerInfo> ordered = ringOrdered();
        if (ordered.size() < 2) return;
        // find successors (next higher IDs)
        java.util.Collections.sort(ordered, new Comparator<WorkerInfo>() { public int compare(WorkerInfo a, WorkerInfo b) { return a.id().compareTo(b.id()); }});
        int idx = -1; for (int i=0;i<ordered.size();i++) if (ordered.get(i).id().equals(selfId)) { idx=i; break; }
        if (idx < 0) return;
        WorkerInfo s1 = ordered.get((idx + 1) % ordered.size());
        WorkerInfo s2 = ordered.get((idx + 2) % ordered.size());
        syncFrom(s1);
        syncFrom(s2);
    }

    private static void syncFrom(WorkerInfo other) {
        try {
            List<String> tables = fetchLines("http://"+other.host()+":"+other.port()+"/repl/tables");
            for (String t : tables) {
                if (t.isBlank()) continue;
                List<String> rows = fetchLines("http://"+other.host()+":"+other.port()+"/repl/rows/"+encode(t));
                for (String line : rows) {
                    if (line.isBlank()) continue;
                    int sp = line.indexOf(' ');
                    if (sp < 0) continue;
                    String key = line.substring(0, sp);
                    // simplistic: if we don't have it, fetch full row
                    Row local = Worker.getRow(t, key);
                    if (local == null) {
                        // download streaming single row
                        // Using whole-row read
                        URL u = new URL("http://"+other.host()+":"+other.port()+"/data/"+encode(t)+"/"+encode(key));
                        HttpURLConnection c = (HttpURLConnection) u.openConnection();
                        c.setConnectTimeout(2000); c.setReadTimeout(2000);
                        if (c.getResponseCode() == 200) {
                            try (var in = c.getInputStream()) {
                                Row fetched = Row.readFrom(in);
                                if (fetched != null) Worker.putRow(t, fetched);
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {}
    }

    private static List<String> fetchLines(String url) throws Exception {
        URL u = new URL(url);
        HttpURLConnection c = (HttpURLConnection) u.openConnection();
        c.setConnectTimeout(2000); c.setReadTimeout(2000);
        List<String> lines = new ArrayList<>();
        try (var in = c.getInputStream(); var sc = new java.util.Scanner(in)) {
            while (sc.hasNextLine()) lines.add(sc.nextLine());
        }
        return lines;
    }

    private static String sha1Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(s.getBytes());
            byte[] dig = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { return ""; }
    }
}
