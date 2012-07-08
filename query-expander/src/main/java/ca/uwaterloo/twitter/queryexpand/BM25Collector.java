package ca.uwaterloo.twitter.queryexpand;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.analysis.TwitterEnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.ItemSetIndexBuilder.AssocField;
import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;
import ca.uwaterloo.twitter.queryexpand.FISQueryExpander.FISCollector.ScoreThenSuppRankComparator;

public abstract class BM25Collector<K extends Comparable<K>, V> extends Collector {
  private static final Logger LOG = LoggerFactory.getLogger(BM25Collector.class);
  
  // Defaults
  // For StemmedIDF = false: binaryTFD = false with b = 0 & k = 1.2 achieves P@30 = 0.3965
  // For StemmedIDF = true: binaryTFD = true with b in [0.2:0.8] & k=0 achieves p@30 = 0.4370
  public static final float K1 = 0.0f; // textbook default 1.2
  public static final float B = 0.2f; // textbook default 0.75
  public static final float LAVG = 9.63676320707029f; // really!
  
  public boolean clarityIDF = true;
  public boolean clarityScore = false;
  public boolean binaryFtd = true;
  
  private static TwitterEnglishAnalyzer stemmingAnalyzer = new TwitterEnglishAnalyzer();
  public final boolean stemmedIDF;
  
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
      Class<? extends Comparator<ScoreIxObj<K>>> comparatorClazz, boolean pStemmedIDF)
      throws IOException, IllegalArgumentException, SecurityException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    
    target = pTarget;
    queryStr = pQueryStr;
    docTextField = pDocTextField;
    stemmedIDF = pStemmedIDF;
    if (pDocTextField.startsWith("stemmed") && !stemmedIDF) {
      throw new IllegalArgumentException("It will be stemmed.. can't avoid it!");
    }
    if (stemmedIDF) {
      MutableLong qLenOut = new MutableLong();
      queryTerms = target.queryTermFreq(queryStr,
          qLenOut,
          stemmingAnalyzer, docTextField);
      queryLen = qLenOut.floatValue();
    } else {
      queryTerms = (OpenObjectFloatHashMap<String>) pQueryTerms.clone();
      queryLen = pQueryLen;
    }
    for (int s = 0; s < addNEnglishStopWordsToQueryTerms; ++s) {
      queryTerms.put(stopWordsEN[s], 1);
    }
    
    Comparator<ScoreIxObj<K>> comparator;
    // if(comparatorClazz == null){
    // comparator = new ScoreThenObjDescComparator();
    // } else {
    comparator = (Comparator<ScoreIxObj<K>>) comparatorClazz.getConstructors()[0].newInstance(this);
    // <ScoreIxObj<K>, V>
    resultSet = new TreeMap(comparator);
    maxResults = pMaxResults;
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }
  
  protected final OpenIntHashSet encounteredDocs = new OpenIntHashSet();
  
  @Override
  public void collect(int docId) throws IOException {
    // float score = scorer.score();
    
    if (encounteredDocs.contains(docId)) {
      LOG.trace("Duplicate document {} for query {}", docId, queryStr);
      return;
    } else {
      encounteredDocs.add(docId);
    }
    
    Document doc = reader.document(docId);
    
    float ld = 0;
    
//    if (stemmedIDF) {
//      // TODONOT if this works store the stemmed vector instead of reparseing the whole index
//      // I wish but there wasn't enough disk space :(
//      String tweet = doc.get(docTextField);
//      MutableLong docLen = new MutableLong();
//      docTerms = target.queryTermFreq(tweet, docLen, stemmingAnalyzer, docTextField);
//      ld = docLen.floatValue();
//    } else {
      TermFreqVector docTermsVector = reader.getTermFreqVector(docId,
          docTextField);
      
      if (!stemmedIDF //FIXME: This is only because the indexes currently don't store the stemmed  
          && docTermsVector != null) {
        docTerms = new OpenObjectFloatHashMap<String>();
        for (int i = 0; i < docTermsVector.size(); ++i) {
          int f = docTermsVector.getTermFrequencies()[i];
          docTerms.put(docTermsVector.getTerms()[i], f);
          ld += f;
        }
      } else {
        MutableLong docLen = new MutableLong();
        if (stemmedIDF) {
          docTerms = target.queryTermFreq(Arrays.toString(docTermsVector.getTerms()),
              docLen, stemmingAnalyzer, docTextField);
        } else {
          String tweet = doc.get(docTextField);
          docTerms = target.queryTermFreq(tweet, docLen);
        }
        ld = docLen.floatValue();
      }
//    }
    
    // BM25
    float score = 0;
    for (String tStr : queryTerms.keys()) {
      float ftd = docTerms.get(tStr);
      if (ftd == 0) {
        continue;
      }
      if (binaryFtd) {
        ftd = 1;
      }
      
      // IDF alwaus comes from the tweet index
      String fieldName;
      if (stemmedIDF) {
        fieldName = TweetField.STEMMED_EN.name;
      } else {
        fieldName = TweetField.TEXT.name;
      }
      Term t = new Term(fieldName, tStr);
      
      // weight of term is its IDF
      
      float idf;
      if (clarityIDF) {
        // // The IDF formula in http://nlp.uned.es/~jperezi/Lucene-BM25/ (used in Clarity)
        idf = target.twtIxReader.docFreq(t);
        idf = (target.twtIxReader.numDocs() - idf + 0.5f) / (idf + 0.5f);
        idf = (float) Math.log(idf); //Slow: MathUtils.log(2, idf);
        
      } else {
        idf = target.twtIxReader.docFreq(t);
        idf = target.twtIxReader.numDocs() / idf;
        idf = (float) Math.log(idf); //slow: MathUtils.log(2, idf);
      }
      
      if (clarityScore) {
        score += idf;
      } else {
        // score += (queryTerms.get(tStr) * ftd * (getK1() + 1) * idf)
        // / ((getK1() * ((1 - getB()) + (getB() * ld / getLAVG()))) + ftd);
        
        // The BM25F formula as per http://nlp.uned.es/~jperezi/Lucene-BM25/
        float wt = ftd / ((1 - getB()) + (getB() * ld / getLAVG()));
        score += (queryTerms.get(tStr) * wt * idf) / (getK1() + wt);
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
