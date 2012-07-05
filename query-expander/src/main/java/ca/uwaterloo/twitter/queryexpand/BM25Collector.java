package ca.uwaterloo.twitter.queryexpand;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;

public class BM25Collector extends Collector {
  private static final Logger LOG = LoggerFactory.getLogger(BM25Collector.class);
  
  public static  float K1 = 1.2f; // default 1.2f;
  public static  float B = 0.0f; // default 0.75
  public static final float LAVG = 9.63676320707029f; // really!
  public  static final int MAX_RESULTS = 10000;
  
  private boolean clarityIDF = false;
  private boolean clarityScore = false;
  
  public class ScoreThenTimeComparator implements Comparator<ScoreIxObj<String>> {
    
    @Override
    public int compare(ScoreIxObj<String> o1, ScoreIxObj<String> o2) {
      int result = o1.compareTo(o2);
      if (result == 0) {
        result = -o1.obj.compareTo(o2.obj);
      }
      return result;
    }
  }
  
  protected Scorer scorer;
  protected IndexReader reader;
  protected int docBase;
  protected final OpenObjectFloatHashMap<String> queryTerms;
  protected final int queryLen;
  protected final TreeMap<ScoreIxObj<String>, String> resultSet;
  protected float maxScore = Float.MIN_VALUE;
  protected float minScore = Float.MAX_VALUE;
  protected final String queryStr;
  private final FISQueryExpander target;
  
  
  public BM25Collector(FISQueryExpander pTarget,
      String pQueryStr, OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen, 
      int addNEnglishStopWordsToQueryTerms)
      throws IOException {
    target = pTarget;
    queryStr = pQueryStr;
    queryTerms = (OpenObjectFloatHashMap<String>) pQueryTerms.clone();
    for(int s = 0; s< addNEnglishStopWordsToQueryTerms; ++s){
      queryTerms.put(stopWordsEN[s], 1);
    }
    queryLen = pQueryLen;
    resultSet = new TreeMap<ScoreIxObj<String>, String>(new ScoreThenTimeComparator());
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }
  
  @Override
  public void collect(int docId) throws IOException {
    // float score = scorer.score();
    
    Document doc = reader.document(docId);
    
    TermFreqVector docTermsVector = reader.getTermFreqVector(docId,
        TweetField.TEXT.name);
    OpenObjectFloatHashMap<String> docTerms;
    float ld = 0;
    if (docTermsVector != null) {
      docTerms = new OpenObjectFloatHashMap<String>();
      for (int i = 0; i < docTermsVector.size(); ++i) {
        int f = docTermsVector.getTermFrequencies()[i];
        docTerms.put(docTermsVector.getTerms()[i], f);
        ld += f;
      }
    } else {
      String tweet = doc.get(TweetField.TEXT.name);
      MutableLong docLen = new MutableLong();
      docTerms = target.queryTermFreq(tweet, docLen);
      ld = docLen.floatValue();
    }
    // BM25
    float score = 0;
    for (String tStr : queryTerms.keys()) {
      float ftd = docTerms.get(tStr);
      if (ftd == 0) {
        continue;
      }
      Term t = new Term(TweetField.TEXT.name, tStr);
      
      // weight of term is its IDF
      
      float idf;
      if (clarityIDF) {
        // // The IDF formula in http://nlp.uned.es/~jperezi/Lucene-BM25/ (used in Clarity)
        idf = target.twtIxReader.docFreq(t);
        idf = (target.twtIxReader.numDocs() - idf + 0.5f) / (idf + 0.5f);
        idf = (float) MathUtils.log(2, idf);
        
      } else {
        idf = target.twtIxReader.docFreq(t);
        idf = target.twtIxReader.numDocs() / idf;
        idf = (float) MathUtils.log(2, idf);
      }
      
      if (clarityScore) {
        score += idf;
      } else {
        // Mutiplying this formula by makes it controllable in the way I want,
        // because I want to make tf negligible, and it saturates to (k+1)*idf as tf -> INF
        score += (queryTerms.get(tStr) * ftd * (getK1() + 1) * idf)
            / ((getK1() * ((1 - getB()) + (getB() * ld / getLAVG()))) + ftd);
        
        // But we have not fields
        // // The BM25F formula as per http://nlp.uned.es/~jperezi/Lucene-BM25/
        // float wt = ftd / ((1-B) + (B * dl / LAVG));
        // bm25 += queryTerms.get(tStr) * (wt * idf) / (K1 + wt);
      }
      
    }
    
    String tweetId = doc.get(TweetField.ID.name);
    String text = null;
    if (LOG.isDebugEnabled()) {
      text = doc.get(TweetField.TEXT.name);
    }
    
    if (score > maxScore) {
      maxScore = score;
    }
    
    if (score < minScore) {
      minScore = score;
    }
    
    resultSet.put(new ScoreIxObj<String>(tweetId, score), text);
    
    while (resultSet.size() > MAX_RESULTS) {
      resultSet.remove(resultSet.lastKey());
    }
  }
  
  float getLAVG() {
    return LAVG;
  }
  
  float getB() {
    return B;
  }
  
  float getK1() {
    return K1;
  }
  
  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.reader = reader;
    this.docBase = docBase;
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }
  
  static final String[] stopWordsEN =
    { "the", "of", "to", "and", "a", "in", "is", "it", "you", "that", "he", "was", "for", "on", "are" };
}
