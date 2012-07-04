package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;

import ca.uwaterloo.twitter.TwitterAnalyzer;

public class TwitterEnglishAnalyzer extends TwitterAnalyzer {
  public class TwitterEnglishTokenStream extends TwitterTokenStream {
    
    PorterStemmer stemmer = new PorterStemmer();
    
    @Override
    public boolean incrementToken() throws IOException {
      boolean result = super.incrementToken();
      if (result) {
        char ch0 = termAtt.buffer()[0];
        if (ch0 != '#' // hashtag
            && ch0 != '@' // username
            && ch0 != 'U' // URL (capital U)
            && !Character.isDigit(ch0) // numbers
        ) {
          if (stemmer.stem(termAtt.buffer(), 0, termAtt.length()))
            termAtt.copyBuffer(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
        }
      }
      return result;
    }
  }
  
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    if (!readInput(reader)) {
      return null;
    }
    return new TwitterEnglishTokenStream();
  }
  
}
