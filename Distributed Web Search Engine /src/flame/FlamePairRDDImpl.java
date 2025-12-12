package flame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kvs.KVSClient;
import kvs.Row;
import tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {

    private String tableName;
    private FlameContextImpl context;

    public FlamePairRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> result = new ArrayList<>();

        // Use shared KVS client
        KVSClient kvs = Coordinator.kvs;

        // Scan whole table
        Iterator<Row> rows = kvs.scan(tableName);

        // Process rows
        while (rows.hasNext()) {
            Row row = rows.next();
            String key = row.key();

            // Emit (key,value) per column
            for (String columnName : row.columns()) {
                String value = row.get(columnName);
                if (value != null) {
                    result.add(new FlamePair(key, value));
                }
            }
        }

        return result;
    }

    @Override
    public int count() throws Exception {
        // Use shared KVS client
        KVSClient kvs = Coordinator.kvs;

        // Count all pairs in the table
        int count = 0;
        Iterator<Row> rows = kvs.scan(tableName);
        
        while (rows.hasNext()) {
            Row row = rows.next();
            count += row.columns().size();
        }
        
        return count;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, flame.FlamePairRDD.TwoStringsToString lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Pass zero element to workers
        Map<String, String> extraParams = new HashMap<>();
        extraParams.put("zeroElement", zeroElement);

        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName, // input table
                null, // output table
                "/pairRdd/foldByKey", // operation route
                serializedLambda, // serialized lambda
                extraParams // extra params with zeroElement
        );

        // Return resulting PairRDD
        return new FlamePairRDDImpl(outputTable, context);
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/pairRdd/flatMap",
                serializedLambda,
                null
        );

        // Return resulting RDD
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Run worker operation
        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/pairRdd/flatMapToPair",
                serializedLambda,
                null
        );

        // Return resulting PairRDD
        return new FlamePairRDDImpl(outputTable, context);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        // Get other table name
        String otherTableName = ((FlamePairRDDImpl) other).tableName;

        // Serialize other table name
        byte[] serializedOtherTable = Serializer.objectToByteArray(otherTableName);

        // Run worker operation with other table
        String outputTable = context.invokeOperation(
                tableName,
                null,
                "/pairRdd/join",
                serializedOtherTable,
                null
        );

        // Return resulting PairRDD
        return new FlamePairRDDImpl(outputTable, context);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        // Use distributed save
        context.invokeOperation(
                tableName,
                tableNameArg,  // Specify the target table name
                "/pairRdd/saveAsTable",
                new byte[0],  // No lambda needed
                null
        );
        
        // Point to saved table
        this.tableName = tableNameArg;
    }

    @Override
    public void forEachPair(PairToVoid lambda) throws Exception {
        // Serialize lambda
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        // Trigger side effects on workers
        context.invokeOperation(
                tableName,
                null,
                "/pairRdd/forEachPair",
                serializedLambda,
                null
        );
    }
}
