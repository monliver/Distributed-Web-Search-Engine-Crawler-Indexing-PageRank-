package flame;

import java.io.Serializable;
import java.util.List;

public interface FlamePairRDD {
  public interface TwoStringsToString extends Serializable {
  	public String op(String a, String b);
  };

  // collect() returns every pair

  public List<FlamePair> collect() throws Exception;

  public int count() throws Exception;

  // foldByKey() reduces values per key using the lambda

	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception;

  // flatMap() emits strings produced from each pair

  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception;

  public interface PairToStringIterable extends Serializable {
    Iterable<String> op(FlamePair pair) throws Exception;
  }

  // flatMapToPair() emits pairs produced from each pair

  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception;

  public interface PairToPairIterable extends Serializable {
    Iterable<FlamePair> op(FlamePair pair) throws Exception;
  }

  // join() pairs matching keys across two RDDs

  public FlamePairRDD join(FlamePairRDD other) throws Exception;
  
  // saveAsTable() writes pairs as rows with column "value"
  public void saveAsTable(String tableNameArg) throws Exception;

  // forEachPair runs a side-effect lambda per pair
  public void forEachPair(PairToVoid lambda) throws Exception;

  public interface PairToVoid extends Serializable {
      void op(FlamePair pair) throws Exception;
  }
}
