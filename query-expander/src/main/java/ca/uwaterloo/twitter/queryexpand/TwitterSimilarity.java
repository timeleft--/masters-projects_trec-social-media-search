package ca.uwaterloo.twitter.queryexpand;

import org.apache.lucene.search.DefaultSimilarity;

public class TwitterSimilarity extends DefaultSimilarity {
  
  /**
   * 
   */
  private static final long serialVersionUID = -4284745532074292161L;

  @Override
  public float tf(float freq) {
    return freq>0?1.0f:0.0f;
  }
  
}
