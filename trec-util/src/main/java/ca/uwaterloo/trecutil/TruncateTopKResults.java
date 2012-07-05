package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class TruncateTopKResults {
  
  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    File resultsFile = new File(args[0]);
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[1])).getChannel(),
        "UTF-8");
    int numRes = Integer.parseInt(args[2]);
    if (numRes <= 0) {
      throw new IllegalArgumentException();
    }
    BufferedReader rd = new BufferedReader(new FileReader(resultsFile));
    String line;
    String currQid = null;
    int currRank = -1;
    while ((line = rd.readLine()) != null) {
      // query_id, iter, docno, rank, sim, run_id
      String[] fields = line.split("\\s");
      if (!fields[0].equals(currQid)) {
        currQid = fields[0];
        currRank = 0;
      }
      if (currRank++ < numRes) {
        wr.append(line + "\n");
      }
    }
    
    wr.flush();
    wr.close();
    
  }
  
}
