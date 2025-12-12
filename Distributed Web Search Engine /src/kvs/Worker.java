package kvs;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;

import tools.Logger;
import webserver.Server;

public class Worker extends generic.Worker {
    
    private static final Logger logger = Logger.getLogger(Worker.class);
    // Storage directory for persistent tables
    private static String storageDir;
    
    // Data structure: Table name -> (Row key -> Row object)
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables = new ConcurrentHashMap<>();
    
    // Versioning data structures: Table name -> (Row key -> version number)
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> rowVersions = new ConcurrentHashMap<>();
    
    // tableName -> rowKey -> (version -> Row)
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Row>>> versionHistory = new ConcurrentHashMap<>();
    
    /** True for pt-* tables. */
    private static boolean isPersistentTable(String tableName) {
        return tableName.startsWith("pt-");
    }
    
    /** Insert row into table. */
    public static void putRow(String tableName, Row row) {
        if (isPersistentTable(tableName)) {
            writeToDisk(tableName, row);
        } else {
            ConcurrentHashMap<String, Row> table = tables.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
            table.put(row.key(), row);
        }
    }
    
    /** Persist row to disk. */
    private static void writeToDisk(String tableName, Row row) {
        try {
            File tableDir = new File(storageDir, tableName);
            if (!tableDir.exists()) {
                tableDir.mkdirs();
            }
            
            String encodedKey = tools.KeyEncoder.encode(row.key());
            File rowFile;
            // subdirectories when encoded file name length >= 6
            if (encodedKey.length() >= 6) {
                String subDirName = "_" + encodedKey.substring(0, 2);
                File subDir = new File(tableDir, subDirName);
                if (!subDir.exists()) {
                    subDir.mkdirs();
                }
                rowFile = new File(subDir, encodedKey);
            } else {
                rowFile = new File(tableDir, encodedKey);
            }
            
            FileOutputStream fos = new FileOutputStream(rowFile);
            fos.write(row.toByteArray());
            fos.close();
            logger.debug("Wrote file " + rowFile.getName() + " to location: " + rowFile.getAbsolutePath());
            System.out.println("Wrote file " + rowFile.getName() + " to location: " + rowFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /** Load persistent row from disk. */
    private static Row readFromDisk(String tableName, String rowKey) {
        try {
            File tableDir = new File(storageDir, tableName);
            if (!tableDir.exists()) {
                return null;
            }
            
            String encodedKey = tools.KeyEncoder.encode(rowKey);
            File rowFile;
            if (encodedKey.length() >= 6) {
                File subDir = new File(tableDir, "_" + encodedKey.substring(0, 2));
                rowFile = new File(subDir, encodedKey);
            } else {
                rowFile = new File(tableDir, encodedKey);
            }

            if (!rowFile.exists() && encodedKey.length() >= 6) {
                File rootFile = new File(tableDir, encodedKey);
                if (rootFile.exists()) {
                    rowFile = rootFile;
                }
            }
            
            if (!rowFile.exists()) {
                return null;
            }
            

            try {
                FileInputStream fis = new FileInputStream(rowFile);
                Row row = Row.readFrom(fis);
                fis.close();
                return row;
            } catch (Exception e) {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    /** Fetch row by key. */
    public static Row getRow(String tableName, String rowKey) {
        if (isPersistentTable(tableName)) {
            return readFromDisk(tableName, rowKey);
        } else {
            ConcurrentHashMap<String, Row> table = tables.get(tableName);
            if (table == null) {
                return null;
            }
            return table.get(rowKey);
        }
    }
    
    /** Fetch row by version. */
    public static Row getRowVersion(String tableName, String rowKey, int version) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Row>> tableHistory = versionHistory.get(tableName);
        if (tableHistory == null) {
            return null;
        }
        ConcurrentHashMap<Integer, Row> rowHistory = tableHistory.get(rowKey);
        if (rowHistory == null) {
            return null;
        }
        return rowHistory.get(version);
    }
    
    /** Current version counter. */
    public static int getCurrentVersion(String tableName, String rowKey) {
        ConcurrentHashMap<String, Integer> tableVersions = rowVersions.get(tableName);
        if (tableVersions == null) {
            return 0;
        }
        return tableVersions.getOrDefault(rowKey, 0);
    }
    
    /** Bump and return next version. */
    public static int getNextVersion(String tableName, String rowKey) {
        ConcurrentHashMap<String, Integer> tableVersions = rowVersions.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
        return tableVersions.compute(rowKey, (key, currentVersion) -> (currentVersion == null) ? 1 : currentVersion + 1);
    }
    
    /** Save row plus version history. */
    public static void putRowWithVersion(String tableName, Row row, int version) {
        putRow(tableName, row);
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Row>> tableHistory = versionHistory.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
        ConcurrentHashMap<Integer, Row> rowHistory = tableHistory.computeIfAbsent(row.key(), k -> new ConcurrentHashMap<>());
        rowHistory.put(version, row.clone());
    }

    // List persistent row files.
    private static java.util.List<File> listRowFiles(File tableDir) {
        java.util.List<File> result = new java.util.ArrayList<>();
        File[] children = tableDir.listFiles();
        if (children == null) return result;
        for (File f : children) {
            if (f.isFile()) {
                // Skip hidden/system files.
                String name = f.getName();
                if (name.startsWith(".")) {
                    continue; // ignore hidden/system files that aren't row data
                }
                result.add(f);
            } else if (f.isDirectory() && f.getName().startsWith("_")) {
                File[] nested = f.listFiles();
                if (nested != null) {
                    for (File nf : nested) if (nf.isFile()) {
                        String name = nf.getName();
                        if (name.startsWith(".")) continue;
                        result.add(nf);
                    }
                }
            }
        }
        return result;
    }

    // List persistent table dirs.
    private static java.util.Set<String> listPersistentTableNames() {
        java.util.Set<String> names = new java.util.TreeSet<>();
        File base = new File(storageDir);
        File[] kids = base.listFiles();
        if (kids == null) return names;
        for (File f : kids) {
            if (f.isDirectory() && f.getName().startsWith("pt-")) {
                names.add(f.getName());
            }
        }
        return names;
    }

    // Recursively delete table dirs.
    private static void deleteRecursive(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) {
                    deleteRecursive(c);
                }
            }
        }
        f.delete();
    }
    
    /** Load or create worker ID file. */
    private static String getOrCreateWorkerId(String storageDirectory) {
        File idFile = new File(storageDirectory, "id");

        if (idFile.exists()) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(idFile));
                String id = reader.readLine();
                reader.close();
                
                if (id != null && !id.trim().isEmpty()) {
                    return id.trim();
                }
            } catch (IOException e) {
            }
        }

        String newId = generateRandomId();
        try {
            File storageDir = new File(storageDirectory);
            if (!storageDir.exists()) {
                storageDir.mkdirs();
            }

            PrintWriter writer = new PrintWriter(new FileWriter(idFile));
            writer.println(newId);
            writer.close();
        } catch (IOException e) {
        }
        
        return newId;
    }
    
    public static void main(String[] args) {
        // Expect <port> <storage_dir> <coordinator>
        if (args.length != 3) {
            System.err.println("Usage: java kvs.Worker <port> <storage_directory> <coordinator_ip:port>");
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
        
        String workerName = args[1];
        String coordinatorAddress = args[2];

        // Use database/kvs_workers/<name>
        String storageDirectory = "database/kvs_workers/" + workerName;
        storageDir = storageDirectory;

        // Ensure storage dir exists
        File storageDirFile = new File(storageDirectory);
        if (!storageDirFile.exists()) {
            storageDirFile.mkdirs();
        }

        // Derive worker ID
        String workerId = getOrCreateWorkerId(storageDirectory);

        // Configure HTTP port
        Server.port(port);

        Server.put("/data/:T", (request, response) -> {
            String tableName = request.params("T");

            byte[] data = request.bodyAsBytes();
            Row row = Row.readFrom(new ByteArrayInputStream(data));

            // Version row before storing
            int newVersion = getNextVersion(tableName, row.key());
            putRowWithVersion(tableName, row, newVersion);

            // Return version header
            response.header("Version", String.valueOf(newVersion));

            return "OK";
        });

        // PUT /data/<table>/<row>/<column> with optional condition
        Server.put("/data/:T/:R/:C", (request, response) -> {
            String tableName = request.params("T");
            String rowKey = request.params("R");
            String columnName = request.params("C");

            byte[] data = request.bodyAsBytes();

            String ifColumn = request.queryParams("ifcolumn");
            String equals = request.queryParams("equals");

            if (ifColumn != null && equals != null) {
                Row existingRow = getRow(tableName, rowKey);
                if (existingRow != null) {
                    String existingValue = existingRow.get(ifColumn);
                    if (existingValue == null || !existingValue.equals(equals)) {
                        return "FAIL"; // Condition not met
                    }
                } else {
                    return "FAIL"; // Row doesn't exist, condition not met
                }
            }

            ConcurrentHashMap<String, Row> table = tables.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
            Row row = table.computeIfAbsent(rowKey, k -> new Row(rowKey));

            row.put(columnName, data);

            int newVersion = getNextVersion(tableName, rowKey);
            putRowWithVersion(tableName, row, newVersion);
            
            // Add version header to response
            response.header("Version", String.valueOf(newVersion));
            
            return "OK";
        });
        
        // PUT /rename/<oldTableName>
        Server.put("/rename/:oldTableName", (request, response) -> {
            String oldTableName = request.params("oldTableName");
            String newTableName = new String(request.bodyAsBytes(), java.nio.charset.StandardCharsets.UTF_8);

            // Return 400 status if one table is persistent (pt-) but the other is not
            if ((oldTableName.startsWith("pt-") && !newTableName.startsWith("pt-")) || 
                (!oldTableName.startsWith("pt-") && newTableName.startsWith("pt-"))) {
                response.status(400, "Bad Request");
                return null;
            }
            
            if (isPersistentTable(oldTableName)) {
                File oldTableDir = new File(storageDir, oldTableName);
                if (!oldTableDir.exists() || !oldTableDir.isDirectory()) {
                    response.status(404, "Not Found");
                    return null;
                }
                
                File newTableDir = new File(storageDir, newTableName);
                if (newTableDir.exists() && newTableDir.isDirectory()) {
                    response.status(409, "Conflict");
                    return null;
                }
                try {
                    java.nio.file.Path root = java.nio.file.Paths.get(storageDir).toAbsolutePath().normalize();
                    java.nio.file.Path src = root.resolve(oldTableName).normalize();
                    java.nio.file.Path dst = src.resolveSibling(newTableName).normalize();
                    java.nio.file.Files.move(src, dst);
                } catch (IOException e) {
                    response.status(500, "Internal Server Error");
                    return "Error renaming table: " + e.getMessage();
                }
            } else {
                if (!tables.containsKey(oldTableName)) {
                    response.status(404, "Not Found");
                    return null;
                }
                if (tables.containsKey(newTableName)) {
                    response.status(409, "Conflict");
                    return null;
                }
                
                ConcurrentHashMap<String, Row> table = tables.get(oldTableName);
                tables.remove(oldTableName);
                tables.put(newTableName, table);
                ConcurrentHashMap<String, Integer> versions = rowVersions.remove(oldTableName);
                if (versions != null) {
                    rowVersions.put(newTableName, versions);
                }
                
                ConcurrentHashMap<String, ConcurrentHashMap<Integer, Row>> history = versionHistory.remove(oldTableName);
                if (history != null) {
                    versionHistory.put(newTableName, history);
                }
            }
            
            return "OK";
        });
        
        // PUT /delete/<tableName>
        Server.put("/delete/:tableName", (request, response) -> {
            String tableName = request.params("tableName");
            
            if (isPersistentTable(tableName)) {
                File tableDir = new File(storageDir, tableName);
                if (!tableDir.exists() || !tableDir.isDirectory()) {
                    response.status(404, "Not Found");
                    return null;
                }
                deleteRecursive(tableDir);
                
            } else {
                if (!tables.containsKey(tableName)) {
                    response.status(404, "Not Found");
                    return null;
                }
                
                tables.remove(tableName);
                rowVersions.remove(tableName);
                versionHistory.remove(tableName);
            }
            
            return "OK";
        });
        
        // GET /data/<table>/<row>
        Server.get("/data/:T/:R", (request, response) -> {
            String tableName = request.params("T");
            String rowKey = request.params("R");

            Row row = getRow(tableName, rowKey);
            if (row == null) {
                response.status(404, "Not Found");
                return "Not Found";
            }

            response.bodyAsBytes(row.toByteArray());
            return row;
        });

        // GET /data/<table>
        Server.get("/data/:T", (request, response) -> {
            String tableName = request.params("T");

            String startRow = request.queryParams("startRow");
            String endRowExclusive = request.queryParams("endRowExclusive");

            response.type("text/plain");

            if (isPersistentTable(tableName)) {
                File tableDir = new File(storageDir, tableName);
                if (!tableDir.exists()) {
                    response.status(404, "Not Found");
                    return "Not Found";
                }
                java.util.List<File> rowFiles = listRowFiles(tableDir);
                for (File file : rowFiles) {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        Row row = Row.readFrom(fis);
                        if (row == null) continue;
                        if (startRow != null && row.key().compareTo(startRow) < 0) continue;
                        if (endRowExclusive != null && row.key().compareTo(endRowExclusive) >= 0) continue;
                        response.write(row.toByteArray());
                        response.write("\n".getBytes());
                    } catch (Exception e) {
                        System.err.println("Error reading file " + file.getName() + ": " + e.getMessage());
                    }
                }
            } else {
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table == null) {
                    response.status(404, "Not Found");
                    return "Not Found";
                }

                for (Row row : table.values()) {
                    if (startRow != null && row.key().compareTo(startRow) < 0) {
                        continue;
                    }
                    if (endRowExclusive != null && row.key().compareTo(endRowExclusive) >= 0) {
                        continue;
                    }
                    response.write(row.toByteArray());
                    response.write("\n".getBytes());
                }
            }

            response.write("\n".getBytes());
            return null;
        });

        // GET /data/<table>/<row>/<column> with versioning
        Server.get("/data/:T/:R/:C", (request, response) -> {
            try {
                String tableName = request.params("T");
                String rowKey = request.params("R");
                String columnName = request.params("C");

                Row row;
                int version;

                String versionParam = request.queryParams("version");
                if (versionParam != null) {
                    try {
                        version = Integer.parseInt(versionParam);
                        row = getRowVersion(tableName, rowKey, version);
                        if (row == null) {
                            response.status(404, "Not Found");
                            return "Not Found";
                        }
                    } catch (NumberFormatException e) {
                        response.status(400, "Bad Request");
                        return "Invalid version number";
                    }
                } else {
                    row = getRow(tableName, rowKey);
                    version = getCurrentVersion(tableName, rowKey);
                    if (row == null) {
                        response.status(404, "Not Found");
                        return "Not Found";
                    }
                }

                byte[] data = row.getBytes(columnName);
                if (data == null) {
                    response.status(404, "Not Found");
                    return "Not Found";
                }

                response.header("Version", String.valueOf(version));

                response.bodyAsBytes(data);
                return data;
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Internal Server Error: " + e.getMessage();
            }
        });


        // DELETE /data/<table>/<row>
        Server.delete("/data/:T/:R", (request, response) -> {
            String tableName = request.params("T");
            String rowKey = request.params("R");
            
            boolean deleted = false;
            
            if (isPersistentTable(tableName)) {
                File tableDir = new File(storageDir, tableName);
                if (!tableDir.exists()) {
                    response.status(404, "Not Found");
                    return "Table not found";
                }

                String encodedKey = tools.KeyEncoder.encode(rowKey);
                File rowFile;
                if (encodedKey.length() >= 6) {
                    File subDir = new File(tableDir, "_" + encodedKey.substring(0, 2));
                    rowFile = new File(subDir, encodedKey);
                } else {
                    rowFile = new File(tableDir, encodedKey);
                }

                if (rowFile.exists() && rowFile.isFile()) {
                    deleted = rowFile.delete();
                    if (!deleted) {
                        response.status(500, "Internal Server Error");
                        return "Failed to delete row file";
                    }
                } else {
                    response.status(404, "Not Found");
                    return "Row not found";
                }
                
            } else {
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table == null) {
                    response.status(404, "Not Found");
                    return "Table not found";
                }
                
                Row removedRow = table.remove(rowKey);
                deleted = (removedRow != null);
                
                if (!deleted) {
                    response.status(404, "Not Found");
                    return "Row not found";
                }

                if (rowVersions.containsKey(tableName)) {
                    rowVersions.get(tableName).remove(rowKey);
                }
                if (versionHistory.containsKey(tableName)) {
                    versionHistory.get(tableName).remove(rowKey);
                }
            }
            
            response.status(200, "OK");
            return "OK";
        });

        // GET /count/<table>
        Server.get("/count/:tableName", (request, response) -> {
            String tableName = request.params("tableName");
            int count = 0;
            
            if (isPersistentTable(tableName)) {
                File tableDir = new File(storageDir, tableName);
                if (tableDir.exists() && tableDir.isDirectory()) {
                    for (File f : listRowFiles(tableDir)) {
                        if (f.isFile()) count++;
                    }
                }
            } else {
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table != null) {
                    count = table.size();
                }
            }
            
            return String.valueOf(count);
        });

        // GET /
        Server.get("/", (request, response) -> {
            response.type("text/html");
            
            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>KVS Worker - Tables</title></head><body>");
            html.append("<h1>Tables</h1>");
            html.append("<table border=\"1\">");
            html.append("<tr><th>Table Name</th><th>Number of Keys</th></tr>");
            
            java.util.Set<String> names = new java.util.TreeSet<>();
            names.addAll(tables.keySet());
            names.addAll(listPersistentTableNames());
            for (String tname : names) {
                int rowCount = 0;
                if (isPersistentTable(tname)) {
                    File tableDir = new File(storageDir, tname);
                    if (tableDir.exists()) {
                        rowCount = listRowFiles(tableDir).size();
                    }
                } else {
                    ConcurrentHashMap<String, Row> table = tables.get(tname);
                    if (table != null) rowCount = table.size();
                }
                html.append("<tr><td><a href=\"/view/").append(tname).append("\">").append(tname).append("</a></td><td>").append(rowCount).append("</td></tr>");
            }
            
            html.append("</table>");
            html.append("</body></html>");
            
            return html.toString();
        });

        // GET /view/<tableName>
        Server.get("/view/:tableName", (request, response) -> {
            response.type("text/html");
            String tableName = request.params("tableName");
            String fromRow = request.queryParams("fromRow");
            
            java.util.List<Row> allRows = new java.util.ArrayList<>();
            boolean tableExists = false;
            
            if (isPersistentTable(tableName)) {
                File tableDir = new File(storageDir, tableName);
                if (tableDir.exists() && tableDir.isDirectory()) {
                    tableExists = true;
                    java.util.List<File> rowFiles = listRowFiles(tableDir);
                    for (File file : rowFiles) {
                        try (FileInputStream fis = new FileInputStream(file)) {
                            Row row = Row.readFrom(fis);
                            if (row != null) allRows.add(row);
                        } catch (Exception e) {
                            System.err.println("Error reading file " + file.getName() + ": " + e.getMessage());
                        }
                    }
                }
            } else {
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table != null) {
                    tableExists = true;
                    allRows.addAll(table.values());
                }
            }
            
            if (!tableExists) {
                response.status(404, "Not Found");
                return "Table not found";
            }

            allRows.sort((r1, r2) -> r1.key().compareTo(r2.key()));
            if (fromRow != null) {
                allRows.removeIf(row -> row.key().compareTo(fromRow) < 0);
            }

            java.util.Set<String> allColumns = new java.util.TreeSet<>();
            for (Row row : allRows) {
                allColumns.addAll(row.columns());
            }
            
            StringBuilder html = new StringBuilder();
            html.append("<html><head><title>Table: ").append(tableName).append("</title></head><body>");
            html.append("<h1>Table: ").append(tableName).append("</h1>");
            html.append("<table border=\"1\">");

            html.append("<tr><th>Row Key</th>");
            for (String column : allColumns) {
                html.append("<th>").append(column).append("</th>");
            }
            html.append("</tr>");

            int rowsDisplayed = 0;
            String nextKey = null;
            
            for (int i = 0; i < allRows.size() && rowsDisplayed < 10; i++) {
                Row row = allRows.get(i);
                html.append("<tr>");
                html.append("<td>").append(row.key()).append("</td>");
                
                for (String column : allColumns) {
                    html.append("<td>");
                    String value = row.get(column);
                    if (value != null) {
                        html.append(value);
                    }
                    html.append("</td>");
                }
                html.append("</tr>");
                rowsDisplayed++;
                if (rowsDisplayed == 10 && i + 1 < allRows.size()) {
                    nextKey = allRows.get(i + 1).key();
                }
            }
            
            html.append("</table>");
            if (nextKey != null) {
                try {
                    String encodedNextKey = java.net.URLEncoder.encode(nextKey, "UTF-8");
                    html.append("<br><a href=\"/view/").append(tableName).append("?fromRow=").append(encodedNextKey).append("\">Next</a>");
                } catch (Exception e) {
                    // Fallback if encoding fails.
                    html.append("<br><a href=\"/view/").append(tableName).append("?fromRow=").append(nextKey).append("\">Next</a>");
                }
            }
            
            html.append("</body></html>");
            
            return html.toString();
        });

        // GET /repl/tables
        Server.get("/repl/tables", (req, res) -> {
            res.type("text/plain");
            StringBuilder sb = new StringBuilder();
            java.util.Set<String> names = new java.util.TreeSet<>();
            names.addAll(tables.keySet());
            names.addAll(listPersistentTableNames());
            for (String n : names) sb.append(n).append("\n");
            return sb.toString();
        });

        // GET /repl/rows/<tableName>
        Server.get("/repl/rows/:tableName", (req, res) -> {
            res.type("text/plain");
            String tableName = req.params("tableName");
            StringBuilder sb = new StringBuilder();
            java.util.List<Row> rows = new java.util.ArrayList<>();
            if (isPersistentTable(tableName)) {
                File tableDir = new File(storageDir, tableName);
                if (tableDir.exists()) {
                    for (File f : listRowFiles(tableDir)) {
                        try (FileInputStream fis = new FileInputStream(f)) {
                            Row r = Row.readFrom(fis); if (r!=null) rows.add(r);
                        } catch (Exception ignored) {}
                    }
                }
            } else {
                ConcurrentHashMap<String, Row> table = tables.get(tableName);
                if (table != null) rows.addAll(table.values());
            }
            rows.sort((a,b)->a.key().compareTo(b.key()));
            for (Row r : rows) {
                sb.append(r.key()).append(" ").append(ReplicationManager.hashRow(r)).append("\n");
            }
            return sb.toString();
        });

        // Configure ping metadata
        setWorkerInfo(coordinatorAddress, port, workerId);

        // Start ping thread
        startPingThread();

        // Start replication refresh
        ReplicationManager.start(coordinatorAddress, workerId, port);
        
        System.out.println("KVS Worker started on port " + port + 
                          ", storage directory: " + storageDirectory + 
                          ", coordinator: " + coordinatorAddress +
                          ", worker ID: " + workerId);

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Worker interrupted, shutting down...");
        }
    }
}