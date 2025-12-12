package flame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import kvs.KVSClient;
import kvs.Row;
import tools.Serializer;

public class FlameRDDImpl implements FlameRDD {

    private String tableName;
    private FlameContextImpl context;

    public FlameRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();

        // Use shared KVS client
        KVSClient kvs = Coordinator.kvs;

        // Scan table
        Iterator<Row> rows = kvs.scan(tableName);

        // Collect column "value"
        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");
            if (value != null) {
                result.add(value);
            }
        }

        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName, // input table
                null, // output table
                "/rdd/flatMap", // operation route
                serializedLambda, // serialized lambda
                null // no extra params
        );
        // Return resulting RDD
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlameRDD flatMapPT(StringToIterable lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Run worker operation
        String outputTable = context.invokeOperationPT(
                tableName, // input table
                null, // output table
                "/rdd/flatMap", // operation route
                serializedLambda, // serialized lambda
                null // no extra params
        );

        // Return resulting RDD
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName, // input table
                null, // output table
                "/rdd/mapToPair", // operation route
                serializedLambda, // serialized lambda
                null // no extra params
        );

        // Return resulting PairRDD
        return new FlamePairRDDImpl(outputTable, context);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        byte[] serializedOtherTable = Serializer.objectToByteArray(((FlameRDDImpl) r).tableName);

        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/rdd/intersection",
                serializedOtherTable,
                null
        );

        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        Map<String, String> extraParams = new HashMap<>();
        extraParams.put("samplingRate", String.valueOf(f));

        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/rdd/sample",
                null,
                extraParams
        );

        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/rdd/groupBy",
                serializedLambda,
                null
        );

        return new FlamePairRDDImpl(outputTable, context);
    }

    @Override
    public int count() throws Exception {
        // Count rows via KVS
        KVSClient kvs = Coordinator.kvs;
        return kvs.count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        // Rename table in KVS
        KVSClient kvs = Coordinator.kvs;
        boolean isSuccess = kvs.rename(tableName, tableNameArg);
        this.tableName = tableNameArg;
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        // Return up to num elements
        Vector<String> result = new Vector<>();
        KVSClient kvs = Coordinator.kvs;
        Iterator<Row> rows = kvs.scan(tableName);

        int count = 0;
        while (rows.hasNext() && count < num) {
            Row row = rows.next();
            String value = row.get("value");
            if (value != null) {
                result.add(value);
                count++;
            }
        }

        return result;
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/rdd/flatMapToPair",
                serializedLambda,
                null
        );

        // Return resulting PairRDD
        return new FlamePairRDDImpl(outputTable, context);
    }

    @Override
    public FlameRDD distinct() throws Exception {
        // No lambda needed for distinct
        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/rdd/distinct",
                null,
                null
        );

        // Return resulting RDD
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public String fold(String zeroElement, flame.FlamePairRDD.TwoStringsToString lambda) throws Exception {
        // Serialize lambda for workers
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Pass zeroElement to workers
        Map<String, String> extraParams = new HashMap<>();
        extraParams.put("zeroElement", zeroElement != null ? zeroElement : "");

        // Gather worker accumulators
        List<String> partials = context.invokeFoldOperation(tableName,
                "/rdd/fold",
                serializedLambda,
                extraParams);

        // Merge partials locally
        String acc = zeroElement;
        for (String part : partials) {
            if (part != null) {
                acc = lambda.op(acc, part);
            }
        }

        return acc;
    }

    @Override
    public FlameRDD filter(StringToBoolean predicate) throws Exception {
        // Serialize predicate
        byte[] serializedLambda = Serializer.objectToByteArray(predicate);

        // Run filter on workers
        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/rdd/filter",
                serializedLambda,
                null
        );

        // Return filtered RDD
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public void forEach(StringToVoid lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Trigger side effects on workers
        context.invokeOperation(
                tableName,
                null,
                "/rdd/forEach",
                serializedLambda,
                null
        );
    }
}
