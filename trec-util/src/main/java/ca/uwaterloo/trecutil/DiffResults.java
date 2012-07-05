package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

public class DiffResults {
  
  /**
   * @param args
   * @throws IOException
   * @throws NumberFormatException
   */
  public static void main(String[] args) throws NumberFormatException, IOException {
    LinkedHashMap<String, Map<String, Float>> baseRes = readResult(new File(args[0]));
    LinkedHashMap<String, Map<String, Float>> otherRes = readResult(new File(args[1]));
    QRelUtil qRelUtil = new QRelUtil(new File(args[2]));
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[3])).getChannel(),
        "UTF-8");
    try {
      wr.append("qid\tdocid\trank-diff\trelevance\n");
      Map<String, Map<String, Float>> allDiff = diffRanks(baseRes, otherRes);
      for (String qid : allDiff.keySet()) {
        LinkedHashMap<String, Float> qRel = qRelUtil.qRel.get(qid);
        if(qRel == null){
          continue; // damn it!  topic 50
        }
        Map<String, Float> qDiff = allDiff.get(qid);
        for (String docId : qDiff.keySet()) {
          wr.append(qid + "\t").append(docId + "\t").append(qDiff.get(docId) + "\t")
              .append(qRel.get(docId) + "\n");
        }
      }
    } finally {
      wr.flush();
      wr.close();
    }
  }
  
  public static Map<String, Map<String, Float>> diffRanks(
      LinkedHashMap<String, Map<String, Float>> base,
      LinkedHashMap<String, Map<String, Float>> other) {
    LinkedHashMap<String, Map<String, Float>> result = new LinkedHashMap<String, Map<String, Float>>();
    
    for (String qid : base.keySet()) {
      Map<String, Float> baseQRes = base.get(qid);
      if(qid.startsWith("MB")){
        qid = Integer.parseInt(qid.substring(2)) + "";
      }
      Map<String, Float> otherQRes = other.get(qid);
      if (otherQRes == null) {
        continue;
        // otherQRes = new HashMap<String, Float>();
      }
      TreeMap<String, Float> rankDiffMap = new TreeMap<String, Float>();
      result.put(qid, rankDiffMap);
      for (String docId : baseQRes.keySet()) {
        Float baseRank = baseQRes.get(docId);
        Float otherRank = otherQRes.get(docId);
        if (otherRank == null) {
          otherRank = Float.POSITIVE_INFINITY;
        }
        rankDiffMap.put(docId, otherRank - baseRank);
      }
    }
    
    return result;
  }
  
  public static LinkedHashMap<String, Map<String, Float>> readResult(File resultsFile)
      throws NumberFormatException, IOException {
    LinkedHashMap<String, Map<String, Float>> result = new LinkedHashMap<String, Map<String, Float>>();
    BufferedReader rd = new BufferedReader(new FileReader(resultsFile));
    String line;
    String currQid = null;
    TreeMap<String, Float> currResult = null;
    // HashMap<String, Float> currRel =null;
    while ((line = rd.readLine()) != null) {
      // query_id, iter, docno, rank, sim, run_id
      String[] fields = line.split("\\s");
      if (!fields[0].equals(currQid)) {
        currQid = fields[0];
        // currRel = qRel.get(currQid);
        currResult = new TreeMap<String, Float>();
        result.put(currQid, currResult);
      }
      currResult.put(fields[2], Float.parseFloat(fields[3]));
    }
    return result;
  }
}
