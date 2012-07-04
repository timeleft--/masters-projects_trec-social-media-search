package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableFloat;

public class QRelUtil {
  final LinkedHashMap<String, LinkedHashMap<String,Float>> qRel;
  final HashSet<String> allRel;
  final LinkedHashMap<String, MutableFloat> sizeOfNonRelevant;
  
  public QRelUtil(File qRelFile) throws IOException {
    qRel = new LinkedHashMap<String, LinkedHashMap<String,Float>>();
    sizeOfNonRelevant = new LinkedHashMap<String, MutableFloat>();
    BufferedReader rd = new BufferedReader(new FileReader(qRelFile));
    MutableFloat currQNonRelevantCount = null;
    
    String line;
    String currQid = null;
    LinkedHashMap<String, Float> currRel =null;
    while((line = rd.readLine()) != null){
      //query_id, iter, docno, rank, sim, run_id
      String[] fields = line.split("\\s");
      if(!fields[0].equals(currQid)){
        currQid = fields[0];
        currRel = new LinkedHashMap<String, Float>();
        qRel.put(currQid, currRel);
        currQNonRelevantCount = new MutableFloat(0);
        sizeOfNonRelevant.put(currQid, currQNonRelevantCount);
      }
      float rel = Float.valueOf(fields[3]);
      currRel.put(fields[2], rel);
      if(rel <= 0){
        currQNonRelevantCount.add(1);
      }
    }
    allRel = new HashSet<String>();
    for(Map<String,Float> rel: qRel.values()){
      allRel.addAll(rel.keySet());
    }
  }
  
  public Map<String,List<String>> findUnjedged(File resultsFile, int numRes) throws IOException{
    if(numRes <= 0){
      numRes = Integer.MAX_VALUE;
    }
    LinkedHashMap<String,List<String>> result = new LinkedHashMap<String, List<String>>();
    BufferedReader rd = new BufferedReader(new FileReader(resultsFile));
    String line;
    String currQid = null;
    List<String> currResult = null;
//    HashMap<String, Float> currRel =null;
    int currRank = -1;
    while((line = rd.readLine()) != null){
    //query_id, iter, docno, rank, sim, run_id
      String[] fields = line.split("\\s");
      if(!fields[0].equals(currQid)){
        currQid = fields[0];
//        currRel = qRel.get(currQid);
        currRank = 0;
        currResult = new LinkedList<String>();
        result.put(currQid, currResult);
      }
//      if(currRel == null || !currRel.keySet().contains(fields[2])){
      if(currRank++ < numRes && !allRel.contains(fields[2])){
        currResult.add(fields[2]);
      }
    }
    return result;
  }
  
 
}
