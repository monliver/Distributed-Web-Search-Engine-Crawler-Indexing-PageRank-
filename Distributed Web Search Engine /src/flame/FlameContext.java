package flame;

import java.io.Serializable;
import java.util.List;

import kvs.KVSClient;

public interface FlameContext {
  public KVSClient getKVS();

  // output() content becomes the /submit response body

  public void output(String s);

  // Return an RDD backed by list elements

  public FlameRDD parallelize(List<String> list) throws Exception;
  public FlameRDD parallelizeToPT(List<String> list) throws Exception;

  // fromTable() maps table rows to strings via lambda

  public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception;

  public interface RowToString extends Serializable {
    String op(kvs.Row row) throws Exception;
  }
}
