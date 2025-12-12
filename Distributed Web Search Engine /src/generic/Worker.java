package generic;

import java.net.URL;
import java.util.Random;

public class Worker {
    
    private static String coordinatorAddress;
    private static int workerPort;
    private static String workerId;
    private static final int PING_INTERVAL_MS = 5000;
    
    public static void setWorkerInfo(String coordinator, int port, String id) {
        coordinatorAddress = coordinator;
        workerPort = port;
        workerId = id;
    }
    
    public static String generateRandomId() {
        Random rand = new Random();
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            id.append((char) ('a' + rand.nextInt(26)));
        }
        return id.toString();
    }
    
    public static void startPingThread(String coordinator, String id, int port) {
        coordinatorAddress = coordinator;
        workerId = id;
        workerPort = port;
        startPingThread();
    }
    
    public static void startPingThread() {
        if (coordinatorAddress == null || workerId == null) {
            System.err.println("Worker configuration not set");
            return;
        }
        
        Thread pingThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(PING_INTERVAL_MS);
                    String pingUrl = "http://" + coordinatorAddress + "/ping?id=" + workerId + "&port=" + workerPort;
                    new URL(pingUrl).getContent();
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    System.err.println("Ping error: " + e.getMessage());
                }
            }
        });
        
        pingThread.setDaemon(true);
        pingThread.start();
    }
}