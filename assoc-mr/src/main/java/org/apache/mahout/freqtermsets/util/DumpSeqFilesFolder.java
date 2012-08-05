package org.apache.mahout.freqtermsets.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.PFPGrowth;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.hadoop.util.CorpusReader;

public class DumpSeqFilesFolder {
  private static Logger L = LoggerFactory.getLogger("DumpSeqFilesFolder");
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) {
    
    // int pHeapSize = 5000;
    File root = new File(args[0]);
    File chunk = root;
//    for(File chunk: root.listFiles())
    for (File hourDir : chunk.listFiles()) {
      for (File minuteDir : hourDir.listFiles()) {
        String minutePath = minuteDir.getAbsolutePath();
        try {
          dumpFrequentPatterns(minutePath + File.separator + PFPGrowth.FREQUENT_PATTERNS,
              minutePath);
          System.out.println("Dumped: " + minutePath);
        } catch (IOException e) {
          System.err.println("Error while processing: " + minutePath + "\n" + e.getMessage());
        }
      }
    }
  }
  
  public static void dumpFrequentPatterns(String seqPath, String outPath) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path inPath = new Path(seqPath);
    if (!fs.exists(inPath)) {
      System.err.println("Error: " + inPath + " does not exist!");
      System.exit(-1);
    }
    
    Path confPath = new Path(inPath, "_logs/history");
    FileStatus[] confFile = fs.listStatus(confPath, new PathFilter() {
      
      public boolean accept(Path p) {
        return p.getName().endsWith("_conf.xml");
      }
    });
    
    StringBuilder outFilename = new StringBuilder("assoc");
    
    Configuration conf = new Configuration();
    if (confFile.length == 0) {
      outFilename.append("_confunknown");
    } else {
      conf.addResource(confFile[0].getPath());
      
      Map<String, String> params = Parameters.parseParams(conf.get("pfp.parameters"));
      params.remove("input");
      params.remove("output");
      params.remove("gfisIn");
      params.remove("countIn");
      params.remove("encoding");
      
      String[] keys = params.keySet().toArray(new String[0]);
      Arrays.sort(keys);
      
      for (String key : keys) {
        outFilename.append('_').append(key).append(params.get(key));
      }
    }
    SimpleDateFormat dateFmt = new SimpleDateFormat("MMddHHmm");
    outFilename.append('_').append(dateFmt.format(new Date()));
    
    PrintStream out = new PrintStream(new FileOutputStream(
        outPath + File.separator + outFilename.toString() + ".csv"), true, "UTF-8");
    
    CorpusReader<Writable, TopKStringPatterns> stream = new CorpusReader<Writable, TopKStringPatterns>(
        inPath, fs, "part.*");
    // HashMap<String, TopKStringPatterns> merged = new HashMap<String, TopKStringPatterns>();
    try {
      
      Pair<Writable, TopKStringPatterns> p;
      while ((p = stream.next()) != null) {
        
        Writable first = p.getFirst();
        TopKStringPatterns second = p.getSecond();
        
        if (second.getPatterns().size() == 0) {
          L.debug("Zero patterns for the feature: {}",
              first.toString());
        } else {
          L.trace(first.toString() + "\t" + second.toString());
          // if(!merged.containsKey(first.toString())){
          // merged.put(first.toString(), new TopKStringPatterns());
          // }
          // TopKStringPatterns m = merged.get(first.toString());
          // m = m.merge(second, pHeapSize);
          // merged.put(first.toString(), m);
          // System.err.println(m.getPatterns());
          
          out.println(first.toString() + "\t" + second.getPatterns().toString());
          
        }
      }
      
    } catch (Exception ex) {
      L.error(ex.getMessage(), ex);
    } finally {
      
      // for(Entry<String, TopKStringPatterns> e: merged.entrySet()){
      // out.println(e.getKey().toString() + "\t" + e.getValue().toString());
      // }
      out.flush();
      out.close();
      stream.close();
    }
  }
  
}
