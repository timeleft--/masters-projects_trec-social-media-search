package ca.uwaterloo.lucene;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.TwitterEnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import com.google.common.collect.ImmutableMap;

import ca.uwaterloo.twitter.TwitterAnalyzer;
import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;

public class AddStats implements Callable<Void> {
  
  private static final Analyzer PLAIN_ANALYZER = new TwitterAnalyzer(); // Version.LUCENE_36);
  private static final Analyzer ENGLISH_ANALYZER = new TwitterEnglishAnalyzer();
  private static final Analyzer ANALYZER = new PerFieldAnalyzerWrapper(PLAIN_ANALYZER,
      ImmutableMap.<String, Analyzer> of(TweetField.STEMMED_EN.name, ENGLISH_ANALYZER));
  
  private static final boolean DONOT_REPLACE = false;
  private static final String NON_STEMMED = TweetField.TEXT.name;
  private static final String STEMMED = TweetField.STEMMED_EN.name;
  private static final int NUM_THREADS = 3;
  
  private static final boolean REPARSE_IF_MISSING = false;
  
  /**
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException,
      ExecutionException {
    File timeRoot = new File(args[0]);
    Future<Void> lastFuture = null;
    ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
    File[] startFiles = timeRoot.listFiles();
    Arrays.sort(startFiles);
    for (File startFolder : startFiles) {
      File[] endFiles = startFolder.listFiles();
      Arrays.sort(endFiles);
      for (File endFolder : endFiles) {
        File statsPath = new File(endFolder, "stats");
        if (statsPath.exists() && DONOT_REPLACE) {
          throw new IllegalArgumentException("Stats already exist: " + statsPath.getAbsolutePath());
        }
        
        AddStats call = new AddStats();
        call.indexRoot = endFolder;
        call.statsPath = statsPath;
        lastFuture = exec.submit(call);
      }
    }
    
    lastFuture.get();
    
    exec.shutdown();
    while (!exec.isTerminated()) {
      Thread.sleep(1000);
    }
  }
  
  public Void call() throws IOException {
    for (Entry<String, SummaryStatistics> e : calcStats(indexRoot).entrySet()) {
      if (e.getValue().getN() == 0) {
        continue;
      }
      FileUtils.writeStringToFile(new File(statsPath, e.getKey()), e.getValue().toString());
    }
    return null;
  }
  
  File indexRoot;
  File statsPath;
  
  public Map<String, SummaryStatistics> calcStats(File indexRoot) throws IOException {
    File indexPath;
    
    if (!"index".equals(indexRoot.getName())) {
      indexPath = new File(indexRoot, "index");
      if (!indexPath.exists()) {
        // This piece of crap deletes everyting after moving when the destination is inside the src
        // FileUtils.moveDirectory(indexRoot, indexPath);
        File[] ixFiles = indexRoot.listFiles(new FileFilter() {
          public boolean accept(File arg0) {
            return arg0.isFile();
          }
        });
        for (File file : ixFiles) {
          FileUtils.moveFileToDirectory(file, indexPath, true);
        }
      }
    } else {
      indexPath = indexRoot;
      indexRoot = indexRoot.getParentFile();
    }
    
    Directory indexDir = NIOFSDirectory.open(indexPath);
    IndexReader ixReader = IndexReader.open(indexDir);
    
    SummaryStatistics docLenTotalRaw = new SummaryStatistics();
    SummaryStatistics docLenUniqueRaw = new SummaryStatistics();
    SummaryStatistics termFreqInDocRaw = new SummaryStatistics();
    SummaryStatistics nullTweetsRaw = new SummaryStatistics();
    
    SummaryStatistics docLenTotalStemmed = new SummaryStatistics();
    SummaryStatistics docLenUniqueStemmed = new SummaryStatistics();
    SummaryStatistics termFreqInDocStemmed = new SummaryStatistics();
    SummaryStatistics nullTweetsStemmed = new SummaryStatistics();
    
    for (int d = 0; d < ixReader.maxDoc(); ++d) {
      TermFreqVector tfv = ixReader.getTermFreqVector(d, NON_STEMMED);
      if (tfv == null && REPARSE_IF_MISSING) {
        Document doc = ixReader.document(d);
        String text = doc.get(NON_STEMMED);
        if (text == null || text.isEmpty()) {
          nullTweetsRaw.addValue(d);
        } else {
          tfv = Parse.createTermFreqVector(text, null, ANALYZER, NON_STEMMED);
        }
      }
      addValues(tfv,
          docLenTotalRaw,
          docLenUniqueRaw,
          termFreqInDocRaw);
      
      tfv = ixReader.getTermFreqVector(d, STEMMED);
      if (tfv == null && REPARSE_IF_MISSING) {
        Document doc = ixReader.document(d);
        String text = doc.get(STEMMED);
        if (text == null || text.isEmpty()) {
          nullTweetsStemmed.addValue(d);
        } else {
          tfv = Parse.createTermFreqVector(text, null, ANALYZER, STEMMED);
        }
      }
      addValues(tfv,
          docLenTotalStemmed,
          docLenUniqueStemmed,
          termFreqInDocStemmed);
    }
    
    Map<String, SummaryStatistics> result = new HashMap<String, SummaryStatistics>();
    result.put("docLenTotalRaw", docLenTotalRaw);
    result.put("docLenUniqueRaw", docLenUniqueRaw);
    result.put("termFreqInDocRaw", termFreqInDocRaw);
    result.put("nullTweetsRaw", nullTweetsRaw);
    
    result.put("docLenTotalStemmed", docLenTotalStemmed);
    result.put("docLenUniqueStemmed", docLenUniqueStemmed);
    result.put("termFreqInDocStemmed", termFreqInDocStemmed);
    result.put("nullTweetsStemmed", nullTweetsStemmed);
    return result;
  }
  
  private static void addValues(TermFreqVector tfv, SummaryStatistics docLenTotal,
      SummaryStatistics docLenUnique, SummaryStatistics termFreqInDoc) {
    if (tfv == null)
      return;
    docLenUnique.addValue(tfv.size());
    int len = 0;
    int[] freq = tfv.getTermFrequencies();
    for (int t = 0; t < freq.length; ++t) {
      len += freq[t];
      termFreqInDoc.addValue(freq[t]);
    }
    docLenTotal.addValue(len);
  }
}
