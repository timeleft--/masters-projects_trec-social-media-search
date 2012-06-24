package org.apache.mahout.freqtermsets.util;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.PFPGrowth;

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
    try{
//    Parameters params = new Parameters();
//    params.set(PFPGrowth.COUNT_IN, inDir);
    
    SummaryStatistics flistStats = new SummaryStatistics();
    for (Pair<String, Long> e : PFPGrowth.readFList(inDir,0,0,100)) {
      wr.append(e.getFirst() + "\t" + e.getSecond() + "\n");
      flistStats.addValue(e.getFirst().length());
    }
    File statF = new File(inDir, "tokens_length-stats.txt");
    FileUtils.writeStringToFile(statF, flistStats.toString());
    }finally{
      wr.flush();
      wr.close();
    }
  }
  
}
