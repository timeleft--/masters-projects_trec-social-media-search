package ca.uwaterloo.twitter;

import org.apache.lucene.search.DefaultSimilarity;

public class TwitterSimilarity extends DefaultSimilarity {
  
  /**
   * 
   */
  private static final long serialVersionUID = -4284745532074292161L;
  
  public boolean clarityIdf = true;

  @Override
  public float tf(float freq) {
    return freq>0?1.0f:0.0f;
  }
  
  @Override
  public float idf(int docFreq, int numDocs) {
    if(clarityIdf){
   // // The IDF formula in http://nlp.uned.es/~jperezi/Lucene-BM25/ (used in Clarity)
      float idf = docFreq;
      idf = (numDocs - idf + 0.5f) / (idf + 0.5f);
      idf = (float) Math.log(idf); // Slow: MathUtils.log(2, idf);
      return idf;
    }
    return super.idf(docFreq, numDocs);
  }
  
}
