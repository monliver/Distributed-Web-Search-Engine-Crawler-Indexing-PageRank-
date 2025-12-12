package tools;

import java.util.*;

public class Partitioner {

  public class Partition {
    public String kvsWorker;
    public String fromKey;
    public String toKeyExclusive;
    public String assignedFlameWorker;

    Partition(String kvsWorkerArg, String fromKeyArg, String toKeyExclusiveArg, String assignedFlameWorkerArg) {
      kvsWorker = kvsWorkerArg;
      fromKey = fromKeyArg;
      toKeyExclusive = toKeyExclusiveArg;
      assignedFlameWorker = assignedFlameWorkerArg;
    }

    Partition(String kvsWorkerArg, String fromKeyArg, String toKeyExclusiveArg) {
      kvsWorker = kvsWorkerArg;
      fromKey = fromKeyArg;
      toKeyExclusive = toKeyExclusiveArg;
      assignedFlameWorker = null;
    }

    public String toString() {
      return "[kvs:"+kvsWorker+", keys: "+(fromKey==null ? "" : fromKey)+"-"+(toKeyExclusive==null ? "" : toKeyExclusive)+", flame: "+assignedFlameWorker+"]";
    }
  };

  boolean sameIP(String a, String b) {
    String aPcs[] = a.split(":");
    String bPcs[] = b.split(":");
    return aPcs[0].equals(bPcs[0]);
  }

  Vector<String> flameWorkers;
  Vector<Partition> partitions;
  boolean alreadyAssigned;
  int keyRangesPerWorker;

  public Partitioner() {
    partitions = new Vector<Partition>();
    flameWorkers = new Vector<String>();
    alreadyAssigned = false;
    keyRangesPerWorker = 1;
  }

  public void setKeyRangesPerWorker(int keyRangesPerWorkerArg) {
    keyRangesPerWorker = keyRangesPerWorkerArg;
  }

  public void addKVSWorker(String kvsWorker, String fromKeyOrNull, String toKeyOrNull) {
  	partitions.add(new Partition(kvsWorker, fromKeyOrNull, toKeyOrNull));
  }

  public void addFlameWorker(String worker) {
  	flameWorkers.add(worker);
  }

  public Vector<Partition> assignPartitions() {
    if (alreadyAssigned || (flameWorkers.size() < 1))
      return null;

    Random rand = new Random();

    /* So far, the 'partitions' vector has one entry for each KVS worker, each of which is responsible for a range of keys. Normally, we would try to 
       evenly assign Flame workers to these ranges, while matching up Flame workers with KVS workers that are on the same machine. But if we happen
       to have fewer KVS partitions than Flame workers, we need to split up some of the KVS partitions first, otherwise some of the Flame workers
       will be idle. */

    while (partitions.size() < flameWorkers.size()) {
      Partition p = partitions.elementAt(rand.nextInt(partitions.size()));
      String split;
      do {
        split = rand.ints(97,123).limit(5).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
      } while (((p.fromKey != null) && (split.compareTo(p.fromKey)<=0)) || ((p.toKeyExclusive != null) && (split.compareTo(p.toKeyExclusive) >= 0)));
      partitions.add(new Partition(p.kvsWorker, split, p.toKeyExclusive));
      p.toKeyExclusive = split;
    }

    /* Now we'll try to evenly assign partitions to workers, giving preference to workers on the same host */

    int numAssigned[] = new int[flameWorkers.size()];
    for (int i=0; i<numAssigned.length; i++)
      numAssigned[i] = 0;

    for (int i=0; i<partitions.size(); i++) {
      int bestCandidate = 0;
      int bestWorkload = 9999;
      for (int j=0; j<numAssigned.length; j++) {
        if ((numAssigned[j] < bestWorkload) || ((numAssigned[j] == bestWorkload) && sameIP(flameWorkers.elementAt(j), partitions.elementAt(i).kvsWorker))) {
          bestCandidate = j;
          bestWorkload = numAssigned[j];
        }
      }

      numAssigned[bestCandidate] ++;
      partitions.elementAt(i).assignedFlameWorker = flameWorkers.elementAt(bestCandidate);
    }

    /* Now split further to achieve the desired level of parallelism */

    while (true) {
      int toSplit = -1;
      for (int i=0; i<numAssigned.length; i++)
        if (numAssigned[i] < keyRangesPerWorker) 
          toSplit = i;

      if (toSplit < 0)
        break;

      Partition p = null;
      do {
        p = partitions.elementAt(rand.nextInt(partitions.size()));
      } while (!p.assignedFlameWorker.equals(flameWorkers.elementAt(toSplit)));

      String split;
      do {
        split = rand.ints(97,123).limit(5).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
      } while (((p.fromKey != null) && (split.compareTo(p.fromKey)<=0)) || ((p.toKeyExclusive != null) && (split.compareTo(p.toKeyExclusive) >= 0)));
      partitions.add(new Partition(p.kvsWorker, split, p.toKeyExclusive, p.assignedFlameWorker));
      p.toKeyExclusive = split;
      numAssigned[toSplit] ++;
    }

    /* Finally, we'll return the partitions to the caller */

    alreadyAssigned = true;
    return partitions;
  }

  public static void main(String args[]) {
    Partitioner p = new Partitioner();
    p.setKeyRangesPerWorker(3);
    p.addKVSWorker("10.0.0.1:1001", null, "def");
    p.addKVSWorker("10.0.0.2:1002", "def", "mno");
    p.addKVSWorker("10.0.0.3:1003", "mno", null);
    p.addFlameWorker("10.0.0.1:2001");
    p.addFlameWorker("10.0.0.2:2002");
    p.addFlameWorker("10.0.0.3:2003");
    Vector<Partition> result = p.assignPartitions();
    for (Partition x : result)
      System.out.println(x);
  }
}