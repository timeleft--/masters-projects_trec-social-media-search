package ca.uwaterloo.twitter.assoc.qexpand;

import org.apache.lucene.search.TopDocs;

public class ScoreIxObj<V> implements Comparable<ScoreIxObj<V>>,
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
  @Override
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
    return obj.toString()+"^"+score;
  }
  
}
