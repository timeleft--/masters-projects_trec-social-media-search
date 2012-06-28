package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QRelUtil {
  final LinkedHashMap<String, LinkedHashMap<String,Float>> qRel;
  final HashSet<String> allRel;
  public QRelUtil(File qRelFile) throws IOException {
    qRel = new LinkedHashMap<String, LinkedHashMap<String,Float>>();
    BufferedReader rd = new BufferedReader(new FileReader(qRelFile));
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
      }
      currRel.put(fields[2], Float.valueOf(fields[3]));
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
  
  public static void main(String[] args) throws IOException {
    QRelUtil app =  new QRelUtil(new File(args[0]));
    Map<String, List<String>> unjudged = app.findUnjedged(new File(args[1]),30);
    PrintStream out = System.out;
    if(args.length == 3){
      out = new PrintStream(new File(args[2]));
    }
    for(String qid: unjudged.keySet()){
      out.println("Qid: " + qid);
      out.println("Num. Unjudged: " + unjudged.get(qid).size());
      out.println("Unjudged Ids: " + unjudged.get(qid));
    }
  }
}
