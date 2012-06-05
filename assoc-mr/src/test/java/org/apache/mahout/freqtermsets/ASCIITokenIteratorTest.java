package org.apache.mahout.freqtermsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.mahout.freqtermsets.TokenIterator.LatinTokenIterator;
import org.junit.Test;

public class ASCIITokenIteratorTest {
  
  @Test
  public void testBasic() {
    TokenIterator target = new TokenIterator("Basic Test");
    assertEquals("basic", target.next());
    assertEquals("test", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void oneWordShort() {
    TokenIterator target = new TokenIterator("OneWord");
    assertEquals("oneword", target.next());
    assertFalse(target.hasNext());
  }
  
//  @Test
//  public void testNoShort() {
//    TokenIterator target = new TokenIterator("no short");
//    assertEquals("short", target.next());
//    assertFalse(target.hasNext());
//  }
  
  @Test
  public void testSymbols() {
    TokenIterator target = new TokenIterator("`~!1@#$%^&*()-_+={}|\\/?><'\":;");
    // 1 makes the length > 3 and token doesn't start with @ or #
    assertEquals("1@#", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testShortenning() {
    TokenIterator target = new TokenIterator("coooooooooooooooooool");
    assertEquals("coool", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testMentions() {
    TokenIterator target = new TokenIterator("@younos");
    assertEquals("@younos", target.next());
    // assertEquals("younos", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testHashtagsNorep() {
    TokenIterator target = new TokenIterator("#hashtag");
    target.setRepeatHashTag(false);
    assertEquals("#hashtag", target.next());
    assertFalse(target.hasNext());
  }

  @Test
  public void testHashtagsRepeat() {
    TokenIterator target = new TokenIterator("#hashtag");
    target.setRepeatHashTag(true);
    assertEquals("#hashtag", target.next());
    assertEquals("hashtag", target.next());
    assertFalse(target.hasNext());
  }
  
  // @Test
  // public void testAsciiOnly(){
  // ASCIITokenIterator target = new ASCIITokenIterator("très جداً");
  // assertEquals("trs", target.next());
  // assertFalse(target.hasNext());
  // }
  
  @Test
  public void testLatinOnly() {
    LatinTokenIterator target = new LatinTokenIterator("très جداً Özsu");
    assertEquals("très", target.next());
    assertEquals("Özsu".toLowerCase(), target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testUrl() {
    LatinTokenIterator target = new LatinTokenIterator("http://youtube.com/dsdf33 www.wikipedia.com https://www.bank.com HTTP://WATCH.THIS");
    assertEquals("URL", target.next());
    assertEquals("URL", target.next());
    assertEquals("URL", target.next());
    assertEquals("URL", target.next());
    assertFalse(target.hasNext());
  }
  
}
