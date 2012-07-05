package ca.uwaterloo.trecutil;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;

public class FindTopKRunsByTrecEval {
  
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    Collection<File> resultFiles = FileUtils.listFiles(new File(args[0]),
        new String[] { "txt" },
        true);
    PriorityQueue<Pair<String, Float>> topKRes = new PriorityQueue<Pair<String, Float>>(1000,
        new Comparator<Pair<String, Float>>() {
          
          public int compare(Pair<String, Float> o1, Pair<String, Float> o2) {
            return -Float.compare(o1.getValue(), o2.getValue());
          }
          
        });
    int numRes = Integer.parseInt(args[3]);
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[2])).getChannel(),
        "UTF-8");
    try {
      for (File resF : resultFiles) {
        wr.append("===============================================").append('\n')
            .append(resF.getAbsolutePath()).append('\n');
        
        Map<String, Float> resMap = new LinkedHashMap<String, Float>();
        if(RunTrecEval.runTrecEval(resF, args[1], resMap, null)!= 0){
          throw new Exception("Non zero result from file: " + resF.getAbsolutePath());
        }
        for (String metric : resMap.keySet()) {
          wr.append(metric).append("\t").append(resMap.get(metric) + "").append('\n');
        }
        
        topKRes.add(new Pair<String, Float>(resF.getAbsolutePath(), resMap.get(args[4])));
        // avgPrecStats.getMean()));
        // rankEffStats.getMean()));
      }
      wr.append("========================= TOP " + numRes + " according to " + args[4]
          + "============================\n");
      int k = 1;
      while (!topKRes.isEmpty()) {
        if (k > numRes) {
          break;
        }
        Pair<String, Float> res = topKRes.poll();
        wr.append(k + ": " + res.getKey() + " - " + args[4] + ": " + res.getValue() + "\n");
        ++k;
      }
      wr.flush();
    } finally {
      wr.flush();
      wr.close();
    }
  }
  
}
