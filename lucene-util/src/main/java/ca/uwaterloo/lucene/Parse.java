package ca.uwaterloo.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.TermFreqVector;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import com.google.common.collect.Lists;

public class Parse {
  public static class MapBackedTermFreqVector implements TermFreqVector {
    final protected OpenObjectIntHashMap<String> termFreqMap;
    final protected String fieldName;
    
    public MapBackedTermFreqVector(OpenObjectIntHashMap<String> termFreqMap, String fieldName) {
      super();
      this.termFreqMap = termFreqMap;
      this.fieldName = fieldName;
    }
    
    public String getField() {
      return fieldName;
    }
    
    public int size() {
      return termFreqMap.size();
    }
    
    public String[] getTerms() {
      List<String> keys = Lists.newArrayListWithCapacity(size());
      termFreqMap.keysSortedByValue(keys);
      
      return keys.toArray(new String[0]);
      
    }
    
    public int[] getTermFrequencies() {
      
      return termFreqMap.values().toArray(new int[0]);
    }
    
    public int indexOf(String term) {
      throw new UnsupportedOperationException();
    }
    
    public int[] indexesOf(String[] terms, int start, int len) {
      throw new UnsupportedOperationException();
    }
  }
  
  public static TermFreqVector createTermFreqVector(String query, MutableLong qLenOut,
      Analyzer pAnalyzer, String pFieldName)
      throws IOException {
    OpenObjectIntHashMap<String> queryFreq = new OpenObjectIntHashMap<String>();
    // String[] queryTokens = query.toString().split("\\W");
    TokenStream queryTokens = pAnalyzer.tokenStream(pFieldName,
        new StringReader(query.toString().trim()));
    queryTokens.reset();
    
    // Set<Term> queryTerms = Sets.newHashSet();
    // parsedQuery.extractTerms(queryTerms);
    while (queryTokens.incrementToken()) {
      CharTermAttribute attr = (CharTermAttribute) queryTokens.getAttribute(queryTokens
          .getAttributeClassesIterator().next());
      String token = attr.toString();
      
      int freq = queryFreq.get(token);
      queryFreq.put(token, ++freq);
      
      if (qLenOut != null) {
        qLenOut.add(1);
      }
    }
    return new MapBackedTermFreqVector(queryFreq, pFieldName);
  }
}
