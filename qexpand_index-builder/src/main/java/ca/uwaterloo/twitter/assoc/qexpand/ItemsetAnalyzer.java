package ca.uwaterloo.twitter.assoc.qexpand;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;

public class ItemsetAnalyzer extends Analyzer {
  private static final Logger LOG = LoggerFactory.getLogger(ItemsetAnalyzer.class);
  
//  private Version matchVersion;
//  
//  public ItemsetAnalyzer(Version pMatchVersion) {
//    this.matchVersion = pMatchVersion;
//  }
  
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    StringBuilder txt = new StringBuilder();
    char[] buff = new char[64*1024];
    int len;
    try {
      while((len =  reader.read(buff)) > 0){
        txt.append(buff, 0, len);
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(),e);
      return null;
    }
    
    final String str = txt.toString();
    
    TokenStream result = new TokenStream() {
      
      private LatinTokenIterator tokenIter;
      
      private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      
      @Override
      public boolean incrementToken() throws IOException {
        boolean result = tokenIter.hasNext();
        if(result){
          termAtt.setEmpty().append(tokenIter.next());
        }
        return result;
      }
      
      @Override
      public void reset() throws IOException {
        tokenIter = new LatinTokenIterator(str);
        tokenIter.setRepeatHashTag(true);
        //TODO tokenIter.setBreakHashTag(true);
        super.reset();
      }
    };
   
    return result;
  }
  
}
