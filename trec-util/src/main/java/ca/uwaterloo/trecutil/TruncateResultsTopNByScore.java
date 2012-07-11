package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public class TruncateResultsTopNByScore {
  
  public static class ScoreIxObj<V> implements Comparable<ScoreIxObj<V>>,
      java.io.Serializable {
    
    /**
 * 
 */
    private static final long serialVersionUID = 3674211884482190944L;
    
    /** The score of this object for the query. */
    public float score;
    
    /** Example: A hit document's number. **/
    public V obj;
    
    /** Only set by {@link TopDocs#merge} */
    public int shardIndex;
    
    public ScoreIxObj(V obj, float score) {
      this(obj, score, -1);
    }
    
    public ScoreIxObj(V obj, float score, int shardIndex) {
      this.obj = obj;
      this.score = score;
      this.shardIndex = shardIndex;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ScoreIxObj<?>) {
        ScoreIxObj<?> o = ((ScoreIxObj<?>) obj);
        return (this.obj.equals(o.obj))
            && (this.shardIndex == o.shardIndex);
      } else {
        return super.equals(obj);
      }
    }
    
    /**
     * Compares based on the score
     */
    // @Override
    public int compareTo(ScoreIxObj<V> o) {
      // -ve because score is assumed to be descending
      return -Double.compare(score, o.score);
    }
    
    @Override
    public int hashCode() {
      if (this.shardIndex != -1) {
        throw new UnsupportedOperationException("Not implemented");
      }
      return this.obj.hashCode();
    }
    
    @Override
    public String toString() {
      return obj.toString() + "^" + score;
    }
    
  }
  
  static final int N = 10000;
  private static final int TWEET_TEXT_TOP_K = 30;
  private static IndexSearcher ixSearcher;
  private static IndexReader ixReader;
  private static Writer wr;
  
  public static void main(String[] args) throws NumberFormatException, IOException {
    BufferedReader rd = new BufferedReader(new FileReader(new File(args[0])));
    wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[1])).getChannel(),
        "UTF-8");
    Directory ixDir = MMapDirectory.open(new File(args[2]));
    ixReader = IndexReader.open(ixDir);
    ixSearcher = new IndexSearcher(ixReader);
    try {
      String line;
      String currQid = null;
      // TreeMap<ScoreIxObj<String>, String> currRel = null;
      PriorityQueue<ScoreIxObj<String>> currRel = null;
      while ((line = rd.readLine()) != null) {
        // query_id, iter, docno, rank, sim, run_id
        String[] fields = line.split("\\s");
        if (!fields[0].equals(currQid)) {
          if (currQid != null) {
            if (currQid.equals("MB076")) {
              wr.append("MB076 00000000000000000 0.0 " + args[4]).append('\n'); // 34922941233762304
            } else {
              writeQueryResults(currQid, currRel, args[3]);
            }
          }
          currQid = fields[0];
          currRel = new PriorityQueue<ScoreIxObj<String>>();
          // new TreeMap<ScoreIxObj<String>, String>();
        }
        Float score = Float.valueOf(fields[2]);
        // currRel.put(new ScoreIxObj<String>(line, score), fields[1]);
        currRel.add(new ScoreIxObj<String>(fields[0] + " " + fields[1] + " " + fields[2] + " " + args[4], score));
      }
      if (currRel != null) {
        writeQueryResults(currQid, currRel, args[3]);
      }
    } finally {
      wr.flush();
      wr.close();
    }
  }
  
  private static void writeQueryResults(String currQid,
      PriorityQueue<ScoreIxObj<String>> currRel, String tweetsDir) throws IOException {
    PriorityQueue<ScoreIxObj<String>> timeSorted = new PriorityQueue<TruncateResultsTopNByScore.ScoreIxObj<String>>();
    Writer actualTweets = Channels.newWriter(FileUtils.openOutputStream(new File(tweetsDir,
        currQid)).getChannel(),
        "UTF-8");
    try {
      int rank = 1;
      // for (ScoreIxObj<String> scoredRes : currRel.keySet()) {
      while (!currRel.isEmpty()) {
        if (rank > N) {
          break;
        }
        ScoreIxObj<String> scoredRes = currRel.poll();
        // String tweetId = currRel.get(scoredRes);
        String tweetId = scoredRes.obj.split("\\s")[1];
        timeSorted.add(new ScoreIxObj<String>(scoredRes.obj, Float.valueOf(tweetId)));
        if (rank <= TWEET_TEXT_TOP_K) {
          TermQuery docQuery = new TermQuery(new Term("id", tweetId));
          TopDocs rs = ixSearcher.search(docQuery, 100);
          
          if (rs.totalHits < 1) {
            continue; // might be a deleted tweet
          } else if (rs.totalHits > 1) {
            throw new IllegalStateException();
          }
          String tweet = ixReader.document(rs.scoreDocs[0].doc).get("text");
          actualTweets
              .append(currQid + "\t" + tweetId + "\t" + scoredRes.score + "\t" + tweet + "\n");
        }
        ++rank;
      }
    } finally {
      actualTweets.flush();
      actualTweets.close();
    }
    
    while (!timeSorted.isEmpty()) {
      wr.append(timeSorted.poll().obj).append('\n');
    }
    wr.flush();
    
  }
}
