package flame;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;
import tools.Serializer;
import static webserver.Server.port;
import static webserver.Server.post;

class Worker extends generic.Worker {

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);
        
        // Ensure worker dir exists
        File flameWorkerDir = new File("database/flame_workers");
        if (!flameWorkerDir.exists()) {
            flameWorkerDir.mkdirs();
            System.out.println("Created flame worker directory: " + flameWorkerDir.getAbsolutePath());
        }
        
        final File myJAR = new File(flameWorkerDir, "__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd/flatMap", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");  // may be null
                String toKeyExclusive = request.queryParams("toKey");  // may be null

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        // Scan whole table
                        rows = kvs.scan(inputTable);
                    } else {
                        // Scan range
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows
                int counter = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null) {
                        // Run lambda
                        Iterable<String> results = lambda.op(value);

                        // Write results
                        if (results != null) {
                            for (String result : results) {
                                // Hash unique row key
                                String rowKey = Hasher.hash(row.key() + "-" + (counter++));
                                kvs.put(outputTable, rowKey, "value", result);
                            }
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        // Execute side-effect lambda on each element
        post("/rdd/forEach", (request, response) -> {
            try {
                String inputTable = request.queryParams("inputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                byte[] lambdaBytes = request.bodyAsBytes();
                FlameRDD.StringToVoid lambda = (FlameRDD.StringToVoid) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                KVSClient kvs = new KVSClient(kvsCoordinator);

                Iterator<Row> rows;
                try {
                    rows = (fromKey == null && toKeyExclusive == null) ? kvs.scan(inputTable) : kvs.scan(inputTable, fromKey, toKeyExclusive);
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");
                    if (value != null) {
                        lambda.op(value);
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/mapToPair", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");  // may be null
                String toKeyExclusive = request.queryParams("toKey");  // may be null

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlameRDD.StringToPair lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        // Scan whole table
                        rows = kvs.scan(inputTable);
                    } else {
                        // Scan range
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null) {
                        // Run lambda to get pair
                        FlamePair pair = lambda.op(value);

                        if (pair != null) {
                            // Map pair to row key + column name (original row key)
                            String pairKey = pair._1();
                            String pairValue = pair._2();
                            String columnName = row.key();

                            kvs.put(outputTable, pairKey, columnName, pairValue);
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/pairRdd/foldByKey", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");  // may be null
                String toKeyExclusive = request.queryParams("toKey");  // may be null
                String zeroElement = request.queryParams("zeroElement");  // initial accumulator

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        // Scan whole table
                        rows = kvs.scan(inputTable);
                    } else {
                        // Scan range
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    System.err.println("[FlameWorker] foldByKey: table not found: " + inputTable
                            + ", kvsCoordinator=" + kvsCoordinator);
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                int rowsScanned = 0;
                int valuesSeen = 0;
                int outputRows = 0;

                // Process rows
                while (rows.hasNext()) {
                    Row row = rows.next();
                    rowsScanned++;
                    String key = row.key();

                    // Seed accumulator
                    String accumulator = zeroElement;

                    // Fold over columns
                    for (String columnName : row.columns()) {
                        String value = row.get(columnName);
                        if (value != null) {
                            valuesSeen++;
                            // Accumulator first arg, value second
                            accumulator = lambda.op(accumulator, value);
                        }
                    }

                    // Store accumulator under key
                    kvs.put(outputTable, key, "value", accumulator);
                    outputRows++;
                }

                System.err.println("[FlameWorker] foldByKey: inputTable=" + inputTable
                        + ", outputTable=" + outputTable
                        + ", fromKey=" + fromKey
                        + ", toKeyExclusive=" + toKeyExclusive
                        + ", rowsScanned=" + rowsScanned
                        + ", valuesSeen=" + valuesSeen
                        + ", outputRows=" + outputRows);

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/intersection", (request, response) -> {
            try {
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKey = request.queryParams("toKey");

                // Read other table name
                String otherTableName = (String) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Cache other table values
                Set<String> otherValues = new HashSet<>();
                try {
                    Iterator<Row> otherRows = kvs.scan(otherTableName);
                    while (otherRows.hasNext()) {
                        Row row = otherRows.next();
                        String value = row.get("value");
                        if (value != null) {
                            otherValues.add(value);
                        }
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + otherTableName;
                }

                // Scan partition for matches
                Set<String> seenValues = new HashSet<>();
                Iterator<Row> rows;
                try {
                    rows = kvs.scan(inputTable, fromKey, toKey);
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null && otherValues.contains(value) && !seenValues.contains(value)) {
                        seenValues.add(value);
                        String rowKey = Hasher.hash(value);
                        kvs.put(outputTable, rowKey, "value", value);
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/sample", (request, response) -> {
            try {
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKey = request.queryParams("toKey");
                double samplingRate = Double.parseDouble(request.queryParams("samplingRate"));

                KVSClient kvs = new KVSClient(kvsCoordinator);
                Iterator<Row> rows;
                try {
                    rows = kvs.scan(inputTable, fromKey, toKey);
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                Random random = new Random();

                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null && random.nextDouble() < samplingRate) {
                        String rowKey = Hasher.hash(row.key() + "-sampled");
                        kvs.put(outputTable, rowKey, "value", value);
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/groupBy", (request, response) -> {
            try {
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKey = request.queryParams("toKey");

                FlameRDD.StringToString lambda = (FlameRDD.StringToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

                KVSClient kvs = new KVSClient(kvsCoordinator);
                Iterator<Row> rows;
                try {
                    rows = kvs.scan(inputTable, fromKey, toKey);
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Group values by lambda key
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null) {
                        String key = lambda.op(value);
                        if (key != null) {
                            // Use original row key as column
                            kvs.put(outputTable, key, row.key(), value);
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/context/fromTable", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlameContext.RowToString lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows
                int counter = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();

                    // Run lambda on row
                    String result = lambda.op(row);

                    // Store resulting string if present
                    if (result != null) {
                        // Hash unique row key
                        String rowKey = Hasher.hash(row.key() + "-" + (counter++));
                        kvs.put(outputTable, rowKey, "value", result);
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/flatMapToPair", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows
                int counter = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null) {
                        // Run lambda to get pairs
                        Iterable<FlamePair> results = lambda.op(value);

                        // Store each pair
                        if (results != null) {
                            for (FlamePair pair : results) {
                                if (pair != null) {
                                    // Use pair key and unique column
                                    String pairKey = pair._1();
                                    String pairValue = pair._2();
                                    // Hash unique column name
                                    String columnName = Hasher.hash(row.key() + "-" + (counter++));
                                    kvs.put(outputTable, pairKey, columnName, pairValue);
                                }
                            }
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/pairRdd/flatMap", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows
                int counter = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String key = row.key();

                    // Iterate over columns for this pair key
                    for (String columnName : row.columns()) {
                        String value = row.get(columnName);
                        if (value != null) {
                            // Build pair and run lambda
                            FlamePair pair = new FlamePair(key, value);
                            Iterable<String> results = lambda.op(pair);

                            // Store each result string
                            if (results != null) {
                                for (String result : results) {
                                    if (result != null) {
                                        // Hash unique row key
                                        String rowKey = Hasher.hash(key + "-" + columnName + "-" + (counter++));
                                        kvs.put(outputTable, rowKey, "value", result);
                                    }
                                }
                            }
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/pairRdd/flatMapToPair", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows
                int counter = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String key = row.key();

                    // Iterate column values for this key
                    for (String columnName : row.columns()) {
                        String value = row.get(columnName);
                        if (value != null) {
                            // Build pair and run lambda
                            FlamePair inputPair = new FlamePair(key, value);
                            Iterable<FlamePair> results = lambda.op(inputPair);

                            // Store each result pair
                            if (results != null) {
                                for (FlamePair resultPair : results) {
                                    if (resultPair != null) {
                                        String pairKey = resultPair._1();
                                        String pairValue = resultPair._2();
                                        // Hash unique column name
                                        String newColumnName = Hasher.hash(key + "-" + columnName + "-" + (counter++));
                                        kvs.put(outputTable, pairKey, newColumnName, pairValue);
                                    }
                                }
                            }
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/distinct", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Use value as row key so duplicates collapse
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null) {
                        // Use value as row key in output table
                        kvs.put(outputTable, value, "value", value);
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/fold", (request, response) -> {
            try {
                String inputTable = request.queryParams("inputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize lambda
                byte[] lambdaBytes = request.bodyAsBytes();
                FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                KVSClient kvs = new KVSClient(kvsCoordinator);

                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Reduce partition values
                String acc = request.queryParams("zeroElement");
                if (acc == null) {
                    acc = "";
                }

                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");
                    if (value != null) {
                        acc = lambda.op(acc, value);
                    }
                }

                // Return accumulator
                response.status(200, "OK");
                return acc;
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/pairRdd/join", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize other table name
                byte[] bodyBytes = request.bodyAsBytes();
                String otherTableName = (String) Serializer.byteArrayToObject(bodyBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                try {
                    if (fromKey == null && toKeyExclusive == null) {
                        rows = kvs.scan(inputTable);
                    } else {
                        rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                    }
                } catch (FileNotFoundException fnf) {
                    response.status(404, "Not Found");
                    return "Table not found: " + inputTable;
                }

                // Process rows from first table
                int counter = 0;
                while (rows.hasNext()) {
                    Row row1 = rows.next();
                    String key = row1.key();

                    // Fetch matching row
                    Row row2 = null;
                    try {
                        row2 = kvs.getRow(otherTableName, key);
                    } catch (FileNotFoundException fnf) {
                        // Skip missing rows
                        continue;
                    }

                    if (row2 == null) {
                        continue;
                    }

                    // Cross join column values
                    for (String col1 : row1.columns()) {
                        String value1 = row1.get(col1);

                        if (value1 != null) {
                            // Combine row values
                            for (String col2 : row2.columns()) {
                                String value2 = row2.get(col2);

                                if (value2 != null) {
                                    // Combine as value1,value2
                                    String combinedValue = value1 + "," + value2;

                                    // Hash column combo
                                    String columnName = Hasher.hash(col1) + "-" + Hasher.hash(col2);

                                    // Store joined value
                                    kvs.put(outputTable, key, columnName, combinedValue);
                                }
                            }
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        post("/rdd/filter", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Deserialize predicate
                byte[] bodyBytes = request.bodyAsBytes();
                FlameRDD.StringToBoolean predicate = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(bodyBytes, myJAR);

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                if (fromKey == null && toKeyExclusive == null) {
                    rows = kvs.scan(inputTable);
                } else {
                    rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                }

                // Apply predicate
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String value = row.get("value");

                    if (value != null) {
                        // Check predicate
                        if (predicate.op(value)) {
                            // Write matches
                            String rowKey = Hasher.hash(value);
                            kvs.put(outputTable, rowKey, "value", value);
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        // PairRDD saveAsTable endpoint
        post("/pairRdd/saveAsTable", (request, response) -> {
            try {
                // Read query params
                String inputTable = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");

                // Connect to KVS
                KVSClient kvs = new KVSClient(kvsCoordinator);

                // Scan input range
                Iterator<Row> rows;
                if (fromKey == null && toKeyExclusive == null) {
                    rows = kvs.scan(inputTable);
                } else {
                    rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                }

                // Process PairRDD table
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String pairKey = row.key();

                    // Iterate column values for the key
                    for (String columnName : row.columns()) {
                        String pairValue = row.get(columnName);
                        if (pairValue != null) {
                            // Write pair value under "value"
                            kvs.put(outputTable, pairKey, "value", pairValue);
                        }
                    }
                }

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

        // Run side-effect lambda per pair
        post("/pairRdd/forEachPair", (request, response) -> {
            System.out.println("[Worker /pairRdd/forEachPair] Route called!");
            try {
                String inputTable = request.queryParams("inputTable");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKey");
                
                System.out.println("[Worker /pairRdd/forEachPair] inputTable=" + inputTable + ", fromKey=" + fromKey + ", toKey=" + toKeyExclusive);

                byte[] lambdaBytes = request.bodyAsBytes();
                FlamePairRDD.PairToVoid lambda = (FlamePairRDD.PairToVoid) Serializer.byteArrayToObject(lambdaBytes, myJAR);

                KVSClient kvs = new KVSClient(kvsCoordinator);

                Iterator<Row> rows;
                if (fromKey == null && toKeyExclusive == null) {
                    rows = kvs.scan(inputTable);
                } else {
                    rows = kvs.scan(inputTable, fromKey, toKeyExclusive);
                }

                // Execute side effect per pair
                int pairCount = 0;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String key = row.key();
                    
                    // Treat each column as a pair value
                    for (String columnName : row.columns()) {
                        String value = row.get(columnName);
                        if (value != null) {
                            FlamePair pair = new FlamePair(key, value);
                            lambda.op(pair);
                            pairCount++;
                            if (pairCount % 1000 == 0) {
                                System.out.println("[Worker /pairRdd/forEachPair] Processed " + pairCount + " pairs");
                            }
                        }
                    }
                }
                
                System.out.println("[Worker /pairRdd/forEachPair] Completed! Total pairs processed: " + pairCount);

                response.status(200, "OK");
                return "OK";
            } catch (FileNotFoundException fnf) {
                System.err.println("[Worker /pairRdd/forEachPair] Table not found: " + fnf.getMessage());
                response.status(404, "Not Found");
                return "Table not found: " + fnf.getMessage();
            } catch (Exception e) {
                System.err.println("[Worker /pairRdd/forEachPair] Error: " + e.getMessage());
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "Error: " + e.getMessage();
            }
        });

    }
}
