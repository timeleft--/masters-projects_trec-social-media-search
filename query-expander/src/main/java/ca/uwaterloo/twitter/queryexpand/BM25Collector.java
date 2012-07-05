package ca.uwaterloo.twitter.queryexpand;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
import ca.uwaterloo.twitter.queryexpand.FISQueryExpander.FISCollector.ScoreThenSuppRankComparator;

public abstract class BM25Collector<K extends Comparable<K>, V> extends Collector {
  private static final Logger LOG = LoggerFactory.getLogger(BM25Collector.class);
  
  // Defaults
  public static final float K1 = 1.2f; // default 1.2f;
  public static final float B = 0.0f; // default 0.75
  public static final float LAVG = 9.63676320707029f; // really!
  
  private final boolean clarityIDF = true;
  private final boolean clarityScore = false;
  
  /**
   * Sorts in descending order. Good if the obj represnts time.
   * 
   * @author yaboulna
   * 
   */
  public class ScoreThenObjDescComparator implements Comparator<ScoreIxObj<K>> {
    
    @Override
    public int compare(ScoreIxObj<K> o1, ScoreIxObj<K> o2) {
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
  protected final float queryLen;
  protected final TreeMap<ScoreIxObj<K>, V> resultSet;
  protected float maxScore = Float.MIN_VALUE;
  protected float minScore = Float.MAX_VALUE;
  protected final String queryStr;
  protected final FISQueryExpander target;
  protected final int maxResults;
  protected final String docTextField;
  protected OpenObjectFloatHashMap<String> docTerms;
  
  public BM25Collector(FISQueryExpander pTarget, String pDocTextField,
      String pQueryStr, OpenObjectFloatHashMap<String> pQueryTerms, float pQueryLen,
      int addNEnglishStopWordsToQueryTerms, int pMaxResults,
      Class<? extends Comparator<ScoreIxObj<K>>> comparatorClazz)
      throws IOException, IllegalArgumentException, SecurityException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    docTextField = pDocTextField;
    target = pTarget;
    queryStr = pQueryStr;
    queryTerms = (OpenObjectFloatHashMap<String>) pQueryTerms.clone();
    for (int s = 0; s < addNEnglishStopWordsToQueryTerms; ++s) {
      queryTerms.put(stopWordsEN[s], 1);
    }
    queryLen = pQueryLen;
    Comparator<ScoreIxObj<K>> comparator;
    // if(comparatorClazz == null){
    // comparator = new ScoreThenObjDescComparator();
    // } else {
    comparator = (Comparator<ScoreIxObj<K>>) comparatorClazz.getConstructors()[0].newInstance(this);
    // <ScoreIxObj<K>, V>
    resultSet = new TreeMap(comparator);
    maxResults = pMaxResults;
  }
  
  public BM25Collector(FISQueryExpander pTarget, String pDocTextField,
      String pQueryStr, OpenObjectFloatHashMap<String> pQueryTerms, float pQueryLen,
      int addNEnglishStopWordsToQueryTerms, int pMaxResults) throws IllegalArgumentException,
      SecurityException, IOException, InstantiationException, IllegalAccessException,
      InvocationTargetException {
    this(pTarget, pDocTextField, pQueryStr, pQueryTerms, pQueryLen,
        addNEnglishStopWordsToQueryTerms, pMaxResults,
        (Class<? extends Comparator<ScoreIxObj<K>>>) ScoreThenObjDescComparator.class);
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
        docTextField);
    float ld = 0;
    if (docTermsVector != null) {
      docTerms = new OpenObjectFloatHashMap<String>();
      for (int i = 0; i < docTermsVector.size(); ++i) {
        int f = docTermsVector.getTermFrequencies()[i];
        docTerms.put(docTermsVector.getTerms()[i], f);
        ld += f;
      }
    } else {
      String tweet = doc.get(docTextField);
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
      
      // IDF alwaus comes from the tweet index
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
    
    if (score > maxScore) {
      maxScore = score;
    }
    
    if (score < minScore) {
      minScore = score;
    }
    
    resultSet.put(new ScoreIxObj<K>(getResultKey(docId, doc), score), getResultValue(docId, doc));
    
    while (resultSet.size() > maxResults) {
      resultSet.remove(resultSet.lastKey());
    }
  }
  
  abstract protected K getResultKey(int docId, Document doc);
  
  abstract protected V getResultValue(int docId, Document doc);
  
  protected float getLAVG() {
    return LAVG;
  }
  
  protected float getB() {
    return B;
  }
  
  protected float getK1() {
    return K1;
  }
  
  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.reader = reader;
    this.docBase = docBase;
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }
  
  public TreeMap<ScoreIxObj<K>, V> getResultSet() {
    return resultSet;
  }
  
  static final String[] stopWordsEN =
  { "the", "of", "to", "and", "a", "in", "is", "it", "you", "that", "he", "was", "for", "on", "are" };
}
