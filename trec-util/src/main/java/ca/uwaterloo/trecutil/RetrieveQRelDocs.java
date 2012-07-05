package ca.uwaterloo.trecutil;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public class RetrieveQRelDocs {
  
  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    QRelUtil qRelUtil = new QRelUtil(new File(args[0]));
    Directory ixDir = MMapDirectory.open(new File(args[1]));
    IndexReader ixReader = IndexReader.open(ixDir);
    IndexSearcher ixSearcher = new IndexSearcher(ixReader);
    int topk = Integer.MAX_VALUE;
    if(args.length > 3){
      topk = Integer.parseInt(args[3]);
    }
    try {
      for (String qid : qRelUtil.qRel.keySet()) {
        Writer wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[2], qid + ".csv"))
            .getChannel(),
            "UTF-8");
        try {
          LinkedHashMap<String, Float> qRel = qRelUtil.qRel.get(qid);
          int rank = 0;
          for (String docId : qRel.keySet()) {
            if(++rank > topk){
              break;
            }
            TermQuery docQuery = new TermQuery(new Term("id", docId));
            TopDocs rs = ixSearcher.search(docQuery, 100);
            
            if (rs.totalHits < 1) {
              continue; // might be a deleted tweet
            } else if (rs.totalHits > 1) {
              throw new IllegalStateException();
            }
            String tweet = ixReader.document(rs.scoreDocs[0].doc).get("text");
            wr.append(qid + "\t" + docId + "\t" + qRel.get(docId) + "\t" + tweet + "\n");
          }
        } finally {
          wr.flush();
          wr.close();
        }
      }
    } finally {
      ixReader.close();
    }
  }
  
}
