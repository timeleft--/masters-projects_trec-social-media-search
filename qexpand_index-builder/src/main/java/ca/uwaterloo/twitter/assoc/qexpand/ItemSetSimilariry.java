package ca.uwaterloo.twitter.assoc.qexpand;

import org.apache.lucene.search.DefaultSimilarity;

public class ItemSetSimilariry extends DefaultSimilarity {
  
  /**
   * 
   */
  private static final long serialVersionUID = 322276253277313104L;

  @Override
  public float sloppyFreq(int distance) {
    return 1.0f;
  }
  
  @Override
  public float tf(float freq) {
    return freq>0?1.0f:0.0f;
  }
  
// Ooops.. totally messed up the results by this  
//  @Override
//  public float coord(int overlap, int maxOverlap) {
//    return overlap;
//  }
}
