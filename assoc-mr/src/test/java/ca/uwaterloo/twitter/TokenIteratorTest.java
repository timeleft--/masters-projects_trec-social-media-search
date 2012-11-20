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
  
  // @Test
  // public void testNoShort() {
  // TokenIterator target = new TokenIterator("no short");
  // assertEquals("short", target.next());
  // assertFalse(target.hasNext());
  // }
  
  @Test
  public void testApostropheMiddle() {
    TokenIterator target = new TokenIterator("Apostrphe shouldn't be 'always delimiter' " +
        "like's ain'tt don't");
    assertEquals("apostrphe", target.next());
    assertEquals("shouldnt", target.next());
    assertEquals("be", target.next());
    assertEquals("always", target.next());
    assertEquals("delimiter", target.next());
    assertEquals("like", target.next());
    assertEquals("s", target.next());
    assertEquals("ain", target.next());
    assertEquals("tt", target.next());
    assertEquals("dont", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testApostropheExtremes() {
    TokenIterator target = new TokenIterator("'quoted'''words'");
    assertEquals("quoted", target.next());
    assertEquals("words", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("''''");
    assertFalse(target.hasNext());
    
    target = new TokenIterator("'t");
    assertEquals("t", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("t'");
    assertEquals("t", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("'t'");
    assertEquals("t", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testSymbols() {
    TokenIterator target = new TokenIterator("`~!1@#$%^&*()-_+={}|\\/?><'\":; you_rock");
    // 1 to insure the token doesn't start with @ or #
    // FIXME: # and @ should be treated as delimiters except in the begining
    assertEquals("1@#", target.next());
    // YA 20121120 Now we don't return tokens made of all symbols: assertEquals("_", target.next());
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
    assertEquals("hashtag", target.next());
    assertEquals("#hashtag", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testHashtagsRepeatAtTheEnd() {
    TokenIterator target = new TokenIterator("#hashtag repeated");
    target.setRepeatHashTag(true);
    target.setRepeatedHashTagAtTheEnd(true);
    assertEquals("hashtag", target.next());
    assertEquals("repeated", target.next());
    assertEquals("#hashtag", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("#hashtag repeated");
    target.setRepeatHashTag(true);
    target.setRepeatedHashTagAtTheEnd(false);
    assertEquals("hashtag", target.next());
    assertEquals("#hashtag", target.next());
    assertEquals("repeated", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("#hashtag1 #hashtag2 repeated");
    target.setRepeatHashTag(true);
    target.setRepeatedHashTagAtTheEnd(true);
    assertEquals("hashtag1", target.next());
    assertEquals("hashtag2", target.next());
    assertEquals("repeated", target.next());
    assertEquals("#hashtag1", target.next());
    assertEquals("#hashtag2", target.next());
    assertFalse(target.hasNext());
  }
  
  @Test
  public void testPoundChar() {
    TokenIterator target = new TokenIterator("#");
    target.setRepeatHashTag(true);
    // YA 20121120 Now we don't return tokens made of all symbols: assertEquals("#", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("##");
    target.setRepeatHashTag(true);
    // YA 20121120 Now we don't return tokens made of all symbols:assertEquals("#", target.next());
    // YA 20121120 Now we don't return tokens made of all symbols:assertEquals("##", target.next());
    assertFalse(target.hasNext());
    
    target = new TokenIterator("##");
    target.setRepeatHashTag(false);
    // YA 20121120 Now we don't return tokens made of all symbols:assertEquals("##", target.next());
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
    LatinTokenIterator target = new LatinTokenIterator(
        "http://youtube.com/dsdf33 OK www.wikipedia.com GOOD https://www.bank.com GOTO HTTP://WATCH.THIS www2012_conference");
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
    LatinTokenIterator target = new LatinTokenIterator(
        "On 05-07/2012 28th birthday buy 3333 for 12,234.99 each ");
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
