package ca.uwaterloo.twitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;

public class TokenIteratorTest {
  
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
    TokenIterator target = new TokenIterator("`~!1@#$%^&*()-_+={}|\\/?><'\":; you_rock");
    // 1 to insure the token doesn't start with @ or #
    assertEquals("1@#", target.next());
    assertEquals("_", target.next());
    assertEquals("you_rock", target.next());
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
    LatinTokenIterator target = new LatinTokenIterator("http://youtube.com/dsdf33 OK www.wikipedia.com GOOD https://www.bank.com GOTO HTTP://WATCH.THIS www2012_conference");
    assertEquals("URL", target.next());
    assertEquals("ok", target.next());
    assertEquals("URL", target.next());
    assertEquals("good", target.next());
    assertEquals("URL", target.next());
    assertEquals("goto", target.next());
    assertEquals("URL", target.next());
    assertEquals("www2012_conference", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testNumbers() {
    LatinTokenIterator target = new LatinTokenIterator("On 05-07/2012 28th birthday buy 3333 for 12,234.99 each ");
    assertEquals("on", target.next());
    assertEquals("05", target.next());
    assertEquals("07", target.next());
    assertEquals("2012", target.next());
    assertEquals("28th", target.next());
    assertEquals("birthday", target.next());
    assertEquals("buy", target.next());
    assertEquals("3333", target.next());
    assertEquals("for", target.next());
    assertEquals("12234.99", target.next());
    assertEquals("each", target.next());
    assertFalse(target.hasNext());
  }
  
}
