package ca.uwaterloo.twitter;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;

public class TwitterAnalyzer extends Analyzer {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterAnalyzer.class);
  
  public class TwitterTokenStream extends TokenStream {
    
    protected LatinTokenIterator tokenIter;
    
    protected final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    
    @Override
    public boolean incrementToken() throws IOException {
      boolean result = tokenIter.hasNext();
      if (result) {
        termAtt.setEmpty().append(tokenIter.next());
      }
      return result;
    }
    
    @Override
    public void reset() throws IOException {
      tokenIter = new LatinTokenIterator(inputStr);
      tokenIter.setRepeatHashTag(true);
      // TODO tokenIter.setBreakHashTag(true);
      super.reset();
    }
  }
  
  
  
  
  private String inputStr;
  
  // private Version matchVersion;
  //
  // public ItemsetAnalyzer(Version pMatchVersion) {
  // this.matchVersion = pMatchVersion;
  // }
  
  protected boolean readInput(Reader reader) {
    StringBuilder txt = new StringBuilder();
    char[] buff = new char[64 * 1024];
    int len;
    try {
      while ((len = reader.read(buff)) > 0) {
        txt.append(buff, 0, len);
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
    
    inputStr = txt.toString();
    return true;
  }
  
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    if (!readInput(reader)) {
      return null;
    }
    
    TokenStream result = new TwitterTokenStream();
    
    return result;
  }
  
}
