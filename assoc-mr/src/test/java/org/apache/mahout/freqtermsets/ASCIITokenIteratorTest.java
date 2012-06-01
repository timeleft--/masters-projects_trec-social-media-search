package org.apache.mahout.freqtermsets;

import org.apache.mahout.freqtermsets.TokenIterator.ASCIITokenIterator;
import org.junit.Test;
import static org.junit.Assert.*; 

public class ASCIITokenIteratorTest {

	@Test
	public void testBasic() {
		TokenIterator target = new TokenIterator("Basic Test");
		assertEquals("basic",target.next());
		assertEquals("test",target.next());
		assertFalse(target.hasNext());
	}
	
	@Test
	public void oneWordShort(){
		TokenIterator target = new TokenIterator("OneWord");
		assertEquals("oneword", target.next());
		assertFalse(target.hasNext());
	}
	
	@Test
	public void testNoShort(){
		TokenIterator target = new TokenIterator("no short");
		assertEquals("short", target.next());
		assertFalse(target.hasNext());
	}

	@Test
	public void testSymbols(){
		TokenIterator target = new TokenIterator("`~!1@#$%^&*()-_+={}|\\/?><'\":;");
		//1 makes the length > 3 and token doesn't start with @ or #
		assertEquals("1@#", target.next());
		assertFalse(target.hasNext());
	}
	
	@Test
	public void testShortenning(){
		TokenIterator target = new TokenIterator("coooooooooooooooooool");
		assertEquals("coool", target.next());
		assertFalse(target.hasNext());
	}
	
	@Test
	public void testMentions(){
		TokenIterator target = new TokenIterator("@younos");
		assertEquals("@younos", target.next());
//		assertEquals("younos", target.next());
		assertFalse(target.hasNext());
	}
	
	@Test
	public void testHashtags(){
		TokenIterator target = new TokenIterator("#hashtag");
		assertEquals("#hashtag", target.next());
		assertEquals("hashtag", target.next());
		assertFalse(target.hasNext());
	}
		
	@Test
	public void testAsciiOnly(){
		ASCIITokenIterator target = new ASCIITokenIterator("très جداً");
		assertEquals("trs", target.next());
		assertFalse(target.hasNext());
	}
	

}
