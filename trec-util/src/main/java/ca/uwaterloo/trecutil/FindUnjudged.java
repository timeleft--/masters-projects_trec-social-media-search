package ca.uwaterloo.trecutil;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public class FindUnjudged {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    
    QRelUtil app = new QRelUtil(new File(args[0]));
    
    Directory ixDir = MMapDirectory.open(new File(args[1]));
    IndexReader ixReader = IndexReader.open(ixDir);
    IndexSearcher ixSearcher = new IndexSearcher(ixReader);
    
    
    Map<String, List<String>> unjudged = app.findUnjedged(new File(args[2]), 30);
    PrintStream out = System.out;
    if (args.length == 4) {
      out = new PrintStream(new File(args[3]));
    }
    
    for (String qid : unjudged.keySet()) {
      out.println("Qid: " + qid);
      out.println("Num. Unjudged: " + unjudged.get(qid).size());
      for (String tweetId : unjudged.get(qid)) {
        TermQuery docQuery = new TermQuery(new Term("id", tweetId));
        TopDocs rs = ixSearcher.search(docQuery, 100);
        
        if (rs.totalHits < 1) {
          continue; // might be a deleted tweet
        } else if (rs.totalHits > 1) {
          throw new IllegalStateException();
        }
        String tweet = ixReader.document(rs.scoreDocs[0].doc).get("text");
        out.println("Unjudged: " + tweetId + "\t" + tweet);
      }
    }
  }
  
}
