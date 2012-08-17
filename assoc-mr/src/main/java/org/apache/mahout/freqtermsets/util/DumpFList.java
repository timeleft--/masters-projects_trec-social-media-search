package org.apache.mahout.freqtermsets.util;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.freqtermsets.PFPGrowth;
import org.apache.mahout.math.map.OpenObjectLongHashMap;

import com.google.common.collect.Lists;

public class DumpFList {
  
  /**
   * @param args
   * @throws IOException
   */
  
  public static void main(String[] args) {
    // String countin = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_0601-2000";
    // String outPath = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/flist.txt";
    // String statPath = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/tokens-length.txt";
    File rootDir = new File(args[0]);
    for (File hourDir : rootDir.listFiles()) {
      for (File minuteDir : hourDir.listFiles()) {
        String minutePath = minuteDir.getAbsolutePath();
        try {
          dumpFlist(minutePath);
          System.out.println("Dumped: " + minutePath);
        } catch (IOException e) {
          System.err.println("Error while processing: " + minutePath + "\n" + e.getMessage());
        }
      }
    }
  }
  
  public static void dumpFlist(String inDir) throws IOException {
    File outF = new File(inDir, "flist_dump.txt");
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(outF).getChannel(), "UTF-8");
    try {
      // Parameters params = new Parameters();
      // params.set(PFPGrowth.COUNT_IN, inDir);
      
      SummaryStatistics flistStats = new SummaryStatistics();
      Configuration conf = new Configuration();
      // for (Pair<String, Long> e : PFPGrowth.readParallelCountingResults(inDir,2,3,100, conf)) {
      // wr.append(e.getFirst() + "\t" + e.getSecond() + "\n");
      OpenObjectLongHashMap<String> freqMap = PFPGrowth.readParallelCountingResults(inDir, conf);
      List<String> termsSorted = Lists.newArrayListWithCapacity(freqMap.size());
      freqMap.keysSortedByValue(termsSorted);
      for (int i = termsSorted.size() - 1; i >= 0; --i) {
        String term = termsSorted.get(i);
        wr.append(term + "\t" + freqMap.get(term) + "\n");
        flistStats.addValue(term.length());
      }
      File statF = new File(inDir, "tokens_length-stats.txt");
      FileUtils.writeStringToFile(statF, flistStats.toString());
    } finally {
      wr.flush();
      wr.close();
    }
  }
  
}
