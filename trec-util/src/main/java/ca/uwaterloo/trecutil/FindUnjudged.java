package ca.uwaterloo.trecutil;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

public class FindUnjudged {
  
  /**
   * @param args
   */
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
