package generic;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import webserver.Server;

public class Coordinator {
    
    private static final int WORKER_TIMEOUT_MS = 15000;
    private static final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    
    protected static class WorkerInfo {
        String id;
        String ip;
        int port;
        long lastPingTime;
        
        WorkerInfo(String id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.lastPingTime = System.currentTimeMillis();
        }
        
        void updatePing() {
            this.lastPingTime = System.currentTimeMillis();
        }
        
        boolean isExpired() {
            return (System.currentTimeMillis() - lastPingTime) > WORKER_TIMEOUT_MS;
        }
    }
    
    public static Vector<String> getWorkers() {
        workers.entrySet().removeIf(entry -> entry.getValue().isExpired());
        Vector<String> result = new Vector<>();
        for (WorkerInfo worker : workers.values()) {
            result.add(worker.ip + ":" + worker.port);
        }
        return result;
    }
    
    public static String workerTable() {
        workers.entrySet().removeIf(entry -> entry.getValue().isExpired());
        
        StringBuilder table = new StringBuilder();
        table.append("<table border='1'>");
        table.append("<tr><th>ID</th><th>IP</th><th>Port</th></tr>");
        
        for (WorkerInfo worker : workers.values()) {
            table.append("<tr>");
            table.append("<td><a href='http://").append(worker.ip).append(":").append(worker.port).append("/'>")
                 .append(worker.id).append("</a></td>");
            table.append("<td>").append(worker.ip).append("</td>");
            table.append("<td>").append(worker.port).append("</td>");
            table.append("</tr>");
        }
        
        table.append("</table>");
        return table.toString();
    }
    
    public static String clientTable() {
        return workerTable();
    }
    
    public static void registerRoutes() {
        Server.get("/ping", (request, response) -> {
            String id = request.queryParams("id");
            String portStr = request.queryParams("port");
            
            if (id == null || portStr == null) {
                response.status(400, "Bad Request");
                return "Missing id or port parameter";
            }
            
            try {
                int port = Integer.parseInt(portStr);
                String ip = request.ip();
                
                WorkerInfo existingWorker = workers.get(id);
                if (existingWorker != null) {
                    existingWorker.ip = ip;
                    existingWorker.port = port;
                    existingWorker.updatePing();
                } else {
                    workers.put(id, new WorkerInfo(id, ip, port));
                }
                
                return "OK";
            } catch (NumberFormatException e) {
                response.status(400, "Bad Request");
                return "Invalid port number";
            }
        });
        
        Server.get("/workers", (request, response) -> {
            workers.entrySet().removeIf(entry -> entry.getValue().isExpired());
            
            StringBuilder result = new StringBuilder();
            result.append(workers.size()).append("\n");
            
            for (WorkerInfo worker : workers.values()) {
                result.append(worker.id).append(",").append(worker.ip).append(":").append(worker.port).append("\n");
            }
            
            return result.toString();
        });
    }
}