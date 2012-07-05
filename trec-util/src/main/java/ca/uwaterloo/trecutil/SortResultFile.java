package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

public class SortResultFile {
  public static void main(String[] args) throws IOException {
    File resultsFile = new File(args[0]);
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[1])).getChannel(),
        "UTF-8");
    LinkedHashMap<String,TreeMap<Float,String>> result = new LinkedHashMap<String, TreeMap<Float,String>>();
    BufferedReader rd = new BufferedReader(new FileReader(resultsFile));
    String line;
    String currQid = null;
    TreeMap<Float,String> currResult = null;
//    HashMap<String, Float> currRel =null;
    while((line = rd.readLine()) != null){
    //query_id, iter, docno, rank, sim, run_id
      String[] fields = line.split("\\s");
      if(!fields[0].equals(currQid)){
        currQid = fields[0];
//        currRel = qRel.get(currQid);
        currResult = new TreeMap<Float,String>();
        result.put(currQid, currResult);
      }
      currResult.put(Float.parseFloat(fields[3]), line);
    }
    for(String qid: result.keySet()){
      TreeMap<Float, String> qRes = result.get(qid);
      for(Float rank: qRes.keySet()){
        wr.append(qRes.get(rank).replace('\t', ' ') + "\n");
      }
    }
  }
}
