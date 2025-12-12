package kvs;

import java.util.*;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import tools.*;

public class KVSClient implements KVS {
  private static final Logger logger = Logger.getLogger(KVSClient.class);

  String coordinator;

  static class WorkerEntry implements Comparable<WorkerEntry> {
    String address;
    String id;

    WorkerEntry(String addressArg, String idArg) {
      address = addressArg;
      id = idArg;
    }

    public int compareTo(WorkerEntry e) {
      return id.compareTo(e.id);
    }
  };

  Vector<WorkerEntry> workers;
  boolean haveWorkers;

  public int numWorkers() throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.size();
  }

  public static String getVersion() {
    return "v1.5 Oct 20 2023";
  }

  public String getCoordinator() {
    return coordinator;
  }

  public String getWorkerAddress(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).address;
  }

  public String getWorkerID(int idx) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    return workers.elementAt(idx).id;
  }

  class KVSIterator implements Iterator<Row> {
    InputStream in;
    boolean atEnd;
    Row nextRow;
    int currentRangeIndex;
    String endRowExclusive;
    String startRow;
    String tableName;
    Vector<String> ranges;

    KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      in = null;
      currentRangeIndex = 0;
      atEnd = false;
      endRowExclusive = endRowExclusiveArg;
      tableName = tableNameArg;
      startRow = startRowArg;
      ranges = new Vector<String>();
      if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
        String url = getURL(tableNameArg, numWorkers()-1, startRowArg, ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0))<0)) ? endRowExclusiveArg : getWorkerID(0));
        ranges.add(url);
      }
      for (int i=0; i<numWorkers(); i++) {
        if ((startRowArg == null) || (i == numWorkers()-1) || (startRowArg.compareTo(getWorkerID(i+1))<0)) {
          if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
            boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(getWorkerID(i))>0);
            boolean useActualEndRow = (endRowExclusiveArg != null) && ((i==(numWorkers()-1)) || (endRowExclusiveArg.compareTo(getWorkerID(i+1))<0));
            String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i), useActualEndRow ? endRowExclusiveArg : ((i<numWorkers()-1) ? getWorkerID(i+1) : null));
            ranges.add(url);
          }
        }
      }

      openConnectionAndFill();
    }

    protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      String params = "";
      if (startRowArg != null)
        params = "startRow="+startRowArg;
      if (endRowExclusiveArg != null)
        params = (params.equals("") ? "" : (params+"&"))+"endRowExclusive="+endRowExclusiveArg;
      return "http://"+getWorkerAddress(workerIndexArg)+"/data/"+tableNameArg+(params.equals("") ? "" : "?"+params);
    }

    void openConnectionAndFill() {
      try {
        if (in != null) {
          in.close();
          in = null;
        }

        if (atEnd)
          return;

        while (true) {
          if (currentRangeIndex >= ranges.size()) {
            atEnd = true;
            return;
          } 

          try {
            URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            in = con.getInputStream();
            Row r = fill();
            if (r != null) {
              nextRow = r;
              break;
            }
          } catch (FileNotFoundException fnfe) {
          } catch (URISyntaxException use) {
          }

          currentRangeIndex ++;
        }
      } catch (IOException ioe) {
        if (in != null) {
          try { in.close(); } catch (Exception e) {}
          in = null;
        }
        atEnd = true;
      }
    }

    synchronized Row fill() {
      try {
        Row r = Row.readFrom(in);
        return r;
      } catch (Exception e) {
        return null;
      }
    }

    public synchronized Row next() {
      if (atEnd)
        return null;
      Row r = nextRow;
      nextRow = fill();
      while ((nextRow == null) && !atEnd) {
        currentRangeIndex ++;
        openConnectionAndFill();
      }
      
      return r;
    }

    public synchronized boolean hasNext() {
      return !atEnd;
    }
  }

  synchronized void downloadWorkers() throws IOException {
    HTTP.Response res = HTTP.doRequest("GET", "http://"+coordinator+"/workers", null);
    if (res == null)
      throw new IOException("Failed to get workers list from coordinator");
    String result = new String(res.body());
    
    String[] pieces = result.split("\n");
    int numWorkers = Integer.parseInt(pieces[0]);
    if (numWorkers < 1)
      throw new IOException("No active KVS workers");
    if (pieces.length != (numWorkers+1))
      throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
    workers.clear();
    for (int i=0; i<numWorkers; i++) {
      String[] pcs = pieces[1+i].split(",");
      workers.add(new WorkerEntry(pcs[1], pcs[0]));
    }
    Collections.sort(workers);

    haveWorkers = true;
  }

  int workerIndexForKey(String key) {
    int chosenWorker = workers.size()-1;
    if (key != null) {
      for (int i=0; i<workers.size()-1; i++) {
        if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i+1).id) < 0))
          chosenWorker = i;
      }
    }

    return chosenWorker;
  }

  public KVSClient(String coordinatorArg) {
    coordinator = coordinatorArg;
    workers = new Vector<WorkerEntry>();
    haveWorkers = false;
  }

  public boolean rename(String oldTableName, String newTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    boolean result = true;
    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/rename/"+java.net.URLEncoder.encode(oldTableName, "UTF-8")+"/", newTableName.getBytes()).body();
        String res = new String(response);
        result &= res.equals("OK");
      } catch (Exception e) {}
    }

    return result;
  }

  public void delete(String oldTableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    for (WorkerEntry w : workers) {
      try {
        byte[] response = HTTP.doRequest("PUT", "http://"+w.address+"/delete/"+java.net.URLEncoder.encode(oldTableName, "UTF-8")+"/", null).body();
        String result = new String(response);
      } catch (Exception e) {}
    }
  }

  public void deleteRow(String tableName, String row) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    if (row == null || row.isEmpty())
      throw new RuntimeException("Row key can't be empty!");

    try {
      String target = "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8");
      HTTP.Response res = HTTP.doRequest("DELETE", target, null);
      if (res == null || res.statusCode() != 200) {
        String msg = res != null ? ("status=" + res.statusCode()) : "null response";
        throw new RuntimeException("DELETE row failed: " + msg + " (" + target + ")");
      }
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    }
  }

  public void put(String tableName, String row, String column, byte value[]) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    try {
      String target = "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8");
      HTTP.Response res = HTTP.doRequest("PUT", target, value);
      if (res == null)
        throw new RuntimeException("PUT request returned null for target: " + target);
      byte[] response = res.body();
      String result = new String(response);
      if (!result.equals("OK")) 
      	throw new RuntimeException("PUT returned something other than OK: "+result+ "("+target+")");

      // logger.debug("Write to " + target + " successful");

    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    }
  }

  public void put(String tableName, String row, String column, String value) throws IOException {
    put(tableName, row, column, value.getBytes());
  }

  private static final ConcurrentHashMap<String, ReentrantLock> APPEND_LOCKS = new ConcurrentHashMap<>();
  private static final int DEFAULT_CHUNK_SIZE = 10;
  private static final String META_COUNT = "__count";
  private static final String META_NEXT_CHUNK = "__nextChunk";
  private static final String META_FULL = "__full";

  private ReentrantLock lockFor(String tableName, String row) {
    String key = tableName + "|" + row;
    APPEND_LOCKS.putIfAbsent(key, new ReentrantLock());
    return APPEND_LOCKS.get(key);
  }

  private int readIntColumn(String tableName, String row, String column, int defaultValue) throws IOException {
    byte[] data = get(tableName, row, column);
    if (data == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(new String(data, StandardCharsets.UTF_8));
    } catch (NumberFormatException nfe) {
      return defaultValue;
    }
  }

  private String readStringColumn(String tableName, String row, String column) throws IOException {
    byte[] data = get(tableName, row, column);
    return data == null ? null : new String(data, StandardCharsets.UTF_8);
  }

  private int countItems(String chunkValue, char sep) {
    if (chunkValue == null || chunkValue.isEmpty()) {
      return 0;
    }
    int count = 1;
    for (int i = 0; i < chunkValue.length(); i++) {
      if (chunkValue.charAt(i) == sep) {
        count++;
      }
    }
    return count;
  }

  private boolean chunkContains(String chunkValue, String candidate, char sep) {
    if (chunkValue == null || chunkValue.isEmpty()) {
      return false;
    }
    String[] parts = chunkValue.split(String.valueOf(sep));
    for (String part : parts) {
      if (part.equals(candidate)) {
        return true;
      }
    }
    return false;
  }

  private String chunkColumnName(String prefix, int index) {
    String actualPrefix = (prefix == null || prefix.isEmpty()) ? "chunk" : prefix;
    return actualPrefix + String.format("%04d", Math.max(index, 0));
  }

  // Manage chunked posting lists with per-word locks.
  public void appendCapped(String tableName, String row, String columnPrefix, String value,
                           int maxItems, char sep) throws IOException {
    appendCapped(tableName, row, columnPrefix, value, maxItems, sep, DEFAULT_CHUNK_SIZE, 0);
  }

  // Variant with custom chunk size and df cap.
  public void appendCapped(String tableName, String row, String columnPrefix, String value,
                           int maxItems, char sep, int chunkSize, int maxDocFrequency) throws IOException {
    if (row == null || row.isEmpty()) {
      throw new RuntimeException("Row key can't be empty!");
    }
    if (value == null || value.isEmpty()) {
      return;
    }
    if (chunkSize <= 0) {
      chunkSize = DEFAULT_CHUNK_SIZE;
    }

    int effectiveCap = maxItems;
    if (maxDocFrequency > 0 && maxDocFrequency < effectiveCap) {
      effectiveCap = maxDocFrequency;
    }
    if (effectiveCap <= 0) {
      return;
    }

    ReentrantLock lock = lockFor(tableName, row);
    lock.lock();
    try {
      String fullFlag = readStringColumn(tableName, row, META_FULL);
      if ("1".equals(fullFlag)) {
        return;
      }

      int currentCount = readIntColumn(tableName, row, META_COUNT, 0);
      if (currentCount >= effectiveCap) {
        put(tableName, row, META_FULL, "1");
        return;
      }

      int nextChunk = readIntColumn(tableName, row, META_NEXT_CHUNK, 0);
      if (nextChunk < 0) {
        nextChunk = 0;
      }

      while (true) {
        if (currentCount >= effectiveCap) {
          put(tableName, row, META_FULL, "1");
          return;
        }

        String chunkColumn = chunkColumnName(columnPrefix, nextChunk);
        String chunkValue = readStringColumn(tableName, row, chunkColumn);

        if (chunkContains(chunkValue, value, sep)) {
          return; // already present
        }

        if (chunkValue == null || chunkValue.isEmpty()) {
          put(tableName, row, chunkColumn, value);
          currentCount++;
          put(tableName, row, META_COUNT, Integer.toString(currentCount));
          put(tableName, row, META_NEXT_CHUNK, Integer.toString(nextChunk));
          if (currentCount >= effectiveCap) {
            put(tableName, row, META_FULL, "1");
          }
          return;
        }

        int chunkCount = countItems(chunkValue, sep);
        if (chunkCount >= chunkSize) {
          nextChunk++;
          put(tableName, row, META_NEXT_CHUNK, Integer.toString(nextChunk));
          continue;
        }

        String updated = chunkValue + sep + value;
        put(tableName, row, chunkColumn, updated);
        currentCount++;
        if (chunkCount + 1 >= chunkSize) {
          nextChunk++;
        }
        put(tableName, row, META_COUNT, Integer.toString(currentCount));
        put(tableName, row, META_NEXT_CHUNK, Integer.toString(nextChunk));
        if (currentCount >= effectiveCap) {
          put(tableName, row, META_FULL, "1");
        }
        return;
      }
    } finally {
      lock.unlock();
    }
  }

  public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();
    if (row.key().equals(""))
      throw new RuntimeException("Row key can't be empty!");

    HTTP.Response res = HTTP.doRequest("PUT", "http://"+workers.elementAt(workerIndexForKey(row.key())).address+"/data/"+tableName, row.toByteArray());
    if (res == null)
      throw new RuntimeException("PUT request returned null for table: " + tableName + ", row: " + row.key());
    byte[] response = res.body();
    String result = new String(response);
    if (!result.equals("OK")) 
      throw new RuntimeException("PUT returned something other than OK: "+result);
  }

  public Row getRow(String tableName, String row) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    if (row.equals(""))
      throw new RuntimeException("Row key can't be empty!");

    HTTP.Response resp = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    if (resp == null)
      throw new IOException("GET request returned null for table: " + tableName + ", row: " + row);
    if (resp.statusCode() == 404)
      return null;

    byte[] result = resp.body();
    try {
      return Row.readFrom(new ByteArrayInputStream(result));
    } catch (Exception e) {
      throw new RuntimeException("Decoding error while reading Row '"+row+"' in table '"+tableName+"' from getRow() URL (encoded as '"+java.net.URLEncoder.encode(row, "UTF-8")+"')");
    }
  }


  @Override
  public byte[] get(String tableName, String row, String column) throws IOException {
    if (!haveWorkers)
      downloadWorkers();
    if (row.equals(""))
      throw new RuntimeException("Row key can't be empty!");

    int workerIdx = workerIndexForKey(row);
    String encodedRow = java.net.URLEncoder.encode(row, "UTF-8");
    String encodedColumn = java.net.URLEncoder.encode(column, "UTF-8");

    // Try the primary worker first
    String workerAddress = workers.elementAt(workerIdx).address;
    String url = "http://"+workerAddress+"/data/"+tableName+"/"+encodedRow+"/"+encodedColumn;
    
    HTTP.Response res = HTTP.doRequest("GET", url, null);
    if (res != null && res.statusCode() == 200) {
      return res.body();
    }

    // Fall back to other workers if primary misses (e.g., rebalancing)
    for (int i = 0; i < workers.size(); i++) {
      if (i == workerIdx) continue; // Skip the one we already tried
      workerAddress = workers.elementAt(i).address;
      url = "http://"+workerAddress+"/data/"+tableName+"/"+encodedRow+"/"+encodedColumn;
      try {
        res = HTTP.doRequest("GET", url, null);
        if (res != null && res.statusCode() == 200) {
          return res.body();
        }
      } catch (Exception e) {
        // Continue to next worker
      }
    }
    
    return null;
  }




  public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    HTTP.Response r = HTTP.doRequest("GET", "http://"+workers.elementAt(workerIndexForKey(row)).address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(row, "UTF-8"), null);
    if (r == null)
      return false;
    return r.statusCode() == 200;
  }

  public int count(String tableName) throws IOException {
    if (!haveWorkers)
      downloadWorkers();

    int total = 0;
    for (WorkerEntry w : workers) {
      HTTP.Response r = HTTP.doRequest("GET", "http://"+w.address+"/count/"+tableName, null);
      if ((r != null) && (r.statusCode() == 200)) {
        String result = new String(r.body());
        total += Integer.valueOf(result).intValue();
      }
    } 
    return total;
  }

  public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
    return scan(tableName, null, null);
  }

  public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
    if (!haveWorkers)
      downloadWorkers();

    return new KVSIterator(tableName, startRow, endRowExclusive);
  }

  public static void main(String args[]) throws Exception {
  	if (args.length < 2) {
      logger.error("Syntax: client <coordinator> get <tableName> <row> <column>");
			logger.error("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
      logger.error("Syntax: client <coordinator> scan <tableName>");
      logger.error("Syntax: client <coordinator> count <tableName>");
      logger.error("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
      logger.error("Syntax: client <coordinator> delete <tableName>");
  		System.exit(1);
  	}

  	KVSClient client = new KVSClient(args[0]);
    if (args[1].equals("put")) {
    	if (args.length != 6) {
	  		logger.error("Syntax: client <coordinator> put <tableName> <row> <column> <value>");
	  		System.exit(1);
    	}
      client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
    } else if (args[1].equals("get")) {
      if (args.length != 5) {
        logger.error("Syntax: client <coordinator> get <tableName> <row> <column>");
        System.exit(1);
      }
      byte[] val = client.get(args[2], args[3], args[4]);
      if (val == null)
        logger.info("No value found");
      else 
        logger.info("Value: " + new String(val, "UTF-8"));
    } else if (args[1].equals("scan")) {
      if (args.length != 3) {
        logger.error("Syntax: client <coordinator> scan <tableName>");
        System.exit(1);
      }

      Iterator<Row> iter = client.scan(args[2], null, null);
      int count = 0;
      while (iter.hasNext()) {
        logger.info(iter.next().toString());
        count ++;
      }
      logger.info(count+" row(s) scanned");
    } else if (args[1].equals("count")) {
      if (args.length != 3) {
        logger.error("Syntax: client <coordinator> count <tableName>");
        System.exit(1);
      }

      logger.info(client.count(args[2])+" row(s) in table '"+args[2]+"'");
    } else if (args[1].equals("delete")) {
      if (args.length != 3) {
        logger.error("Syntax: client <coordinator> delete <tableName>");
        System.exit(1);
      }

      client.delete(args[2]);
      logger.info("Table '"+args[2]+"' deleted");
    } else if (args[1].equals("rename")) {
      if (args.length != 4) {
        logger.error("Syntax: client <coordinator> rename <oldTableName> <newTableName>");
        System.exit(1);
      }
      if (client.rename(args[2], args[3]))
        logger.info("Success");
      else
        logger.info("Failure");
    } else {
    	logger.error("Unknown command: "+args[1]);
    	System.exit(1);
    }
  }
};