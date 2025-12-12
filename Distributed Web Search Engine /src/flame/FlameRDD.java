package flame;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

public interface FlameRDD {

    public interface StringToIterable extends Serializable {

        Iterable<String> op(String a) throws Exception;
    };

    public interface StringToPair extends Serializable {

        FlamePair op(String a) throws Exception;
    };

    public interface StringToPairIterable extends Serializable {

        Iterable<FlamePair> op(String a) throws Exception;
    };

    public interface StringToString extends Serializable {

        String op(String a) throws Exception;
    };

    public List<String> collect() throws Exception;

    public FlameRDD flatMap(StringToIterable lambda) throws Exception;
    public FlameRDD flatMapPT(StringToIterable lambda) throws Exception;

    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception;

    public FlameRDD intersection(FlameRDD r) throws Exception;

    public FlameRDD sample(double f) throws Exception;

    public FlamePairRDD groupBy(StringToString lambda) throws Exception;

    public int count() throws Exception;

    public void saveAsTable(String tableNameArg) throws Exception;

    public Vector<String> take(int num) throws Exception;

    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception;

    public FlameRDD distinct() throws Exception;

    public String fold(String zeroElement, flame.FlamePairRDD.TwoStringsToString lambda) throws Exception;

    public FlameRDD filter(StringToBoolean predicate) throws Exception;

    public interface StringToBoolean extends Serializable {

        boolean op(String s) throws Exception;
    }

    // forEach runs a side-effect lambda per element
    public void forEach(StringToVoid lambda) throws Exception;

    public interface StringToVoid extends Serializable {
        void op(String s) throws Exception;
    }
}
