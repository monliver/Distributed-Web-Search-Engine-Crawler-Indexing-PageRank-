package flame;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import kvs.KVSClient;
import tools.HTTP;
import tools.Hasher;
import tools.Partitioner;
import tools.Partitioner.Partition;
import tools.Serializer;

public class FlameContextImpl implements FlameContext {

    private String jarName;
    private StringBuilder outputBuilder;
    private int tableCounter;

    public FlameContextImpl(String jarName) {
        this.jarName = jarName;
        this.outputBuilder = new StringBuilder();
        this.tableCounter = 0;
    }

    @Override
    public KVSClient getKVS() {
        return Coordinator.kvs;
    }

    @Override
    public void output(String s) {
        outputBuilder.append(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        // Create unique table name
        String tableName = "rdd-" + System.currentTimeMillis() + "-" + (tableCounter++);

        // Use shared KVS
        KVSClient kvs = getKVS();

        // Store each string with unique row key
        int index = 0;
        for (String value : list) {
            // Hash index for row key
            String rowKey = Hasher.hash(String.valueOf(index++));
            // Write to column "value"
            kvs.put(tableName, rowKey, "value", value);
        }

        // Return resulting RDD
        return new FlameRDDImpl(tableName, this);
    }

    @Override
    public FlameRDD parallelizeToPT(List<String> list) throws Exception {
        // Create unique table name
        String tableName = "pt-rdd-" + System.currentTimeMillis() + "-" + (tableCounter++);

        // Use shared KVS
        KVSClient kvs = getKVS();

        // Store each string with unique row key
        int index = 0;
        for (String value : list) {
            // Hash index for row key
            String rowKey = Hasher.hash(String.valueOf(index++));
            // Write to column "value"
            kvs.put(tableName, rowKey, "value", value);
        }

        // Return resulting RDD
        return new FlameRDDImpl(tableName, this);
    }

    public String getOutput() {
        if (outputBuilder.length() == 0) {
            return "No output";
        }
        return outputBuilder.toString();
    }

    /** Invoke worker route */
    public String invokeOperation(String inputTableName, String outputTableName,
            String operationRoute, byte[] lambda,
            Map<String, String> extraParams) throws Exception {
        // Create output table if needed
        final String finalOutputTableName;
        if (outputTableName == null) {
            finalOutputTableName = "rdd-" + System.currentTimeMillis() + "-" + (tableCounter++);
        } else {
            finalOutputTableName = outputTableName;
        }

        // Use shared KVS
        KVSClient kvs = getKVS();

        // Build partitioner
        Partitioner partitioner = new Partitioner();

        // Register KVS workers
        int numKVSWorkers = kvs.numWorkers();

        for (int i = 0; i < numKVSWorkers; i++) {
            String workerAddress = kvs.getWorkerAddress(i);
            String workerId = kvs.getWorkerID(i);

            if (i == numKVSWorkers - 1) {
                // Last worker covers wrap-around
                String firstWorkerId = kvs.getWorkerID(0);
                partitioner.addKVSWorker(workerAddress, workerId, null); // High range
                partitioner.addKVSWorker(workerAddress, null, firstWorkerId); // Low range
            } else {
                // Mid workers cover bounded range
                String nextWorkerId = kvs.getWorkerID(i + 1);
                partitioner.addKVSWorker(workerAddress, workerId, nextWorkerId);
            }
        }

        // Register Flame workers
        Vector<String> flameWorkers = Coordinator.getWorkers();
        for (String worker : flameWorkers) {
            partitioner.addFlameWorker(worker);
        }

        // Assign partitions
        Vector<Partition> partitions = partitioner.assignPartitions();

        if (partitions == null || partitions.isEmpty()) {
            System.err.println("[FlameContextImpl] invokeOperation: no partitions assigned for route "
                    + operationRoute + ", inputTable=" + inputTableName
                    + ", kvsWorkers=" + numKVSWorkers + ", flameWorkers=" + flameWorkers.size());
            throw new Exception("Flame invokeOperation failed: no workers/partitions available for " + operationRoute);
        }

        // Prepare parallel requests
        Thread[] threads = new Thread[partitions.size()];
        HTTP.Response[] responses = new HTTP.Response[partitions.size()];
        Exception[] exceptions = new Exception[partitions.size()];

        for (int i = 0; i < partitions.size(); i++) {
            final int index = i;
            final Partition partition = partitions.get(i);

            threads[i] = new Thread(() -> {
                try {
                    // Build worker URL
                    StringBuilder urlBuilder = new StringBuilder();
                    urlBuilder.append("http://").append(partition.assignedFlameWorker).append(operationRoute);
                    urlBuilder.append("?inputTable=").append(URLEncoder.encode(inputTableName, "UTF-8"));
                    urlBuilder.append("&outputTable=").append(URLEncoder.encode(finalOutputTableName, "UTF-8"));
                    urlBuilder.append("&kvsCoordinator=").append(URLEncoder.encode(kvs.getCoordinator(), "UTF-8"));

                    // Append key bounds when present
                    if (partition.fromKey != null) {
                        urlBuilder.append("&fromKey=").append(URLEncoder.encode(partition.fromKey, "UTF-8"));
                    }
                    if (partition.toKeyExclusive != null) {
                        urlBuilder.append("&toKey=").append(URLEncoder.encode(partition.toKeyExclusive, "UTF-8"));
                    }

                    // Append optional params
                    if (extraParams != null) {
                        for (Map.Entry<String, String> entry : extraParams.entrySet()) {
                            urlBuilder.append("&").append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                            urlBuilder.append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                        }
                    }

                    String url = urlBuilder.toString();

                    // POST serialized lambda if present
                    responses[index] = HTTP.doRequest("POST", url, lambda);

                } catch (Exception e) {
                    exceptions[index] = e;
                }
            });

            threads[i].start();
        }

        // Join threads
        for (Thread thread : threads) {
            thread.join();
        }

        // Check results
        for (int i = 0; i < responses.length; i++) {
            if (exceptions[i] != null) {
                throw new Exception("Worker request failed: " + exceptions[i].getMessage(), exceptions[i]);
            }
            if (responses[i] == null || responses[i].statusCode() != 200) {
                String body = responses[i] != null ? new String(responses[i].body()) : "null response";
                throw new Exception("Worker returned status "
                        + (responses[i] != null ? responses[i].statusCode() : "null")
                        + ": " + body);
            }
        }

        return finalOutputTableName;
    }

    public String invokeOperationPT(String inputTableName, String outputTableName,
            String operationRoute, byte[] lambda,
            Map<String, String> extraParams) throws Exception {
        // Create output table if needed
        final String finalOutputTableName;
        if (outputTableName == null) {
            finalOutputTableName = "pt-rdd-" + System.currentTimeMillis() + "-" + (tableCounter++);
        } else {
            finalOutputTableName = outputTableName;
        }

        // Use shared KVS
        KVSClient kvs = getKVS();

        // Build partitioner
        Partitioner partitioner = new Partitioner();

        // Register KVS workers
        int numKVSWorkers = kvs.numWorkers();

        for (int i = 0; i < numKVSWorkers; i++) {
            String workerAddress = kvs.getWorkerAddress(i);
            String workerId = kvs.getWorkerID(i);

            if (i == numKVSWorkers - 1) {
                // Last worker covers wrap-around
                String firstWorkerId = kvs.getWorkerID(0);
                partitioner.addKVSWorker(workerAddress, workerId, null); // High range
                partitioner.addKVSWorker(workerAddress, null, firstWorkerId); // Low range
            } else {
                // Mid workers cover bounded range
                String nextWorkerId = kvs.getWorkerID(i + 1);
                partitioner.addKVSWorker(workerAddress, workerId, nextWorkerId);
            }
        }

        // Register Flame workers
        Vector<String> flameWorkers = Coordinator.getWorkers();
        for (String worker : flameWorkers) {
            partitioner.addFlameWorker(worker);
        }

        // Assign partitions
        Vector<Partition> partitions = partitioner.assignPartitions();

        if (partitions == null || partitions.isEmpty()) {
            System.err.println("[FlameContextImpl] invokeOperationPT: no partitions assigned for route "
                    + operationRoute + ", inputTable=" + inputTableName
                    + ", kvsWorkers=" + numKVSWorkers + ", flameWorkers=" + flameWorkers.size());
            throw new Exception("Flame invokeOperationPT failed: no workers/partitions available for " + operationRoute);
        }

        // Prepare parallel requests
        Thread[] threads = new Thread[partitions.size()];
        HTTP.Response[] responses = new HTTP.Response[partitions.size()];
        Exception[] exceptions = new Exception[partitions.size()];

        for (int i = 0; i < partitions.size(); i++) {
            final int index = i;
            final Partition partition = partitions.get(i);

            threads[i] = new Thread(() -> {
                try {
                    // Build worker URL
                    StringBuilder urlBuilder = new StringBuilder();
                    urlBuilder.append("http://").append(partition.assignedFlameWorker).append(operationRoute);
                    urlBuilder.append("?inputTable=").append(URLEncoder.encode(inputTableName, "UTF-8"));
                    urlBuilder.append("&outputTable=").append(URLEncoder.encode(finalOutputTableName, "UTF-8"));
                    urlBuilder.append("&kvsCoordinator=").append(URLEncoder.encode(kvs.getCoordinator(), "UTF-8"));

                    // Append key bounds when present
                    if (partition.fromKey != null) {
                        urlBuilder.append("&fromKey=").append(URLEncoder.encode(partition.fromKey, "UTF-8"));
                    }
                    if (partition.toKeyExclusive != null) {
                        urlBuilder.append("&toKey=").append(URLEncoder.encode(partition.toKeyExclusive, "UTF-8"));
                    }

                    // Append optional params
                    if (extraParams != null) {
                        for (Map.Entry<String, String> entry : extraParams.entrySet()) {
                            urlBuilder.append("&").append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                            urlBuilder.append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                        }
                    }

                    String url = urlBuilder.toString();

                    // POST serialized lambda if present
                    responses[index] = HTTP.doRequest("POST", url, lambda);

                } catch (Exception e) {
                    exceptions[index] = e;
                }
            });

            threads[i].start();
        }

        // Join threads
        for (Thread thread : threads) {
            thread.join();
        }

        // Check results
        for (int i = 0; i < responses.length; i++) {
            if (exceptions[i] != null) {
                throw new Exception("Worker request failed: " + exceptions[i].getMessage(), exceptions[i]);
            }
            if (responses[i] == null || responses[i].statusCode() != 200) {
                String body = responses[i] != null ? new String(responses[i].body()) : "null response";
                throw new Exception("Worker returned status "
                        + (responses[i] != null ? responses[i].statusCode() : "null")
                        + ": " + body);
            }
        }

        return finalOutputTableName;
    }

    // Invoke fold on workers and gather partial accumulators
    public List<String> invokeFoldOperation(String inputTableName,
            String operationRoute,
            byte[] lambda,
            Map<String, String> extraParams) throws Exception {
        // Use shared KVS
        KVSClient kvs = getKVS();

        // Build partitioner
        Partitioner partitioner = new Partitioner();
        int numKVSWorkers = kvs.numWorkers();

        for (int i = 0; i < numKVSWorkers; i++) {
            String workerAddress = kvs.getWorkerAddress(i);
            String workerId = kvs.getWorkerID(i);
            if (i == numKVSWorkers - 1) {
                String firstWorkerId = kvs.getWorkerID(0);
                partitioner.addKVSWorker(workerAddress, workerId, null);
                partitioner.addKVSWorker(workerAddress, null, firstWorkerId);
            } else {
                String nextWorkerId = kvs.getWorkerID(i + 1);
                partitioner.addKVSWorker(workerAddress, workerId, nextWorkerId);
            }
        }

        Vector<String> flameWorkers = Coordinator.getWorkers();
        for (String worker : flameWorkers) {
            partitioner.addFlameWorker(worker);
        }

        Vector<Partition> partitions = partitioner.assignPartitions();
        if (partitions == null || partitions.isEmpty()) {
            throw new Exception("No partitions were assigned!");
        }

        Thread[] threads = new Thread[partitions.size()];
        HTTP.Response[] responses = new HTTP.Response[partitions.size()];
        Exception[] exceptions = new Exception[partitions.size()];

        for (int i = 0; i < partitions.size(); i++) {
            final int index = i;
            final Partition partition = partitions.get(i);
            threads[i] = new Thread(() -> {
                try {
                    StringBuilder urlBuilder = new StringBuilder();
                    urlBuilder.append("http://").append(partition.assignedFlameWorker).append(operationRoute);
                    urlBuilder.append("?inputTable=").append(URLEncoder.encode(inputTableName, "UTF-8"));
                    urlBuilder.append("&kvsCoordinator=").append(URLEncoder.encode(kvs.getCoordinator(), "UTF-8"));
                    if (partition.fromKey != null) {
                        urlBuilder.append("&fromKey=").append(URLEncoder.encode(partition.fromKey, "UTF-8"));
                    }
                    if (partition.toKeyExclusive != null) {
                        urlBuilder.append("&toKey=").append(URLEncoder.encode(partition.toKeyExclusive, "UTF-8"));
                    }
                    if (extraParams != null) {
                        for (Map.Entry<String, String> entry : extraParams.entrySet()) {
                            urlBuilder.append("&").append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                            urlBuilder.append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                        }
                    }
                    String url = urlBuilder.toString();
                    responses[index] = HTTP.doRequest("POST", url, lambda);
                } catch (Exception e) {
                    exceptions[index] = e;
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        List<String> bodies = new ArrayList<>();
        for (int i = 0; i < responses.length; i++) {
            if (exceptions[i] != null) {
                throw new Exception("Worker request failed: " + exceptions[i].getMessage(), exceptions[i]);
            }
            if (responses[i] == null || responses[i].statusCode() != 200) {
                String body = responses[i] != null ? new String(responses[i].body()) : "null response";
                throw new Exception("Worker returned status "
                        + (responses[i] != null ? responses[i].statusCode() : "null")
                        + ": " + body);
            }
            byte[] bodyBytes = responses[i].body();
            if (bodyBytes == null) {
                bodies.add("");
            } else {
                bodies.add(new String(bodyBytes));
            }
        }

        return bodies;
    }

    public String getJarName() {
        return jarName;
    }

    @Override
    public FlameRDD fromTable(String tableName, FlameContext.RowToString lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Run worker operation
        String outputTable = invokeOperation(
                tableName, // input table
                null, // output table (will be generated)
                "/context/fromTable", // operation route
                serializedLambda, // serialized lambda
                null // no extra params
        );

        // Return resulting RDD
        return new FlameRDDImpl(outputTable, this);
    }
}
