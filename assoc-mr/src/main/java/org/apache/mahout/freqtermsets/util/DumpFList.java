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
  
  public static void main(String[] args) throws IOException {
    String countin = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_0601-2000";
    String outPath = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/flist.txt";
    String statPath = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/tokens-length.txt";
    File outF = new File(outPath);
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(outF).getChannel(), "UTF-8");
    Parameters params = new Parameters(); 
    params.set(PFPGrowth.COUNT_IN, countin);
    
    SummaryStatistics flistStats = new SummaryStatistics();
    for (Pair<String, Long> e : PFPGrowth.readFList(params)) {
      wr.append(e.getFirst() + "\t" + e.getSecond() + "\n");
      flistStats.addValue(e.getFirst().length());
    }
    File statF = new File(statPath);
    FileUtils.writeStringToFile(statF, flistStats.toString());
  }
  
}
