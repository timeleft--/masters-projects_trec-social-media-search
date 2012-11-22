package org.apache.mahout.freqtermsets;

import org.apache.mahout.common.Pair;
import org.apache.mahout.math.list.IntArrayList;
import org.junit.Before;
import org.junit.Test;

import com.ibm.icu.impl.PatternTokenizer;

import static org.junit.Assert.*;

public class TransactionTreeTest {

	TransactionTree target;

	@Before
	public void setup() {
		target = new TransactionTree();

		IntArrayList pattern = new IntArrayList();
		pattern.add(1);
		pattern.add(3);
		pattern.add(5);
		pattern.add(7);
		target.addPattern(pattern, 3);

		pattern = new IntArrayList();
		pattern.add(2);
		pattern.add(4);
		pattern.add(6);
		pattern.add(8);
		target.addPattern(pattern, 4);
	}

//	@Test
	public void testSameUniquePatterns() {
		IntArrayList pattern = new IntArrayList();
		pattern.add(1);
		pattern.add(3);
		pattern.add(5);
		pattern.add(7);
		int numNodes = target.addPatternUnique(pattern, 6);
		assertEquals("No nodes should have been added", 0, numNodes);

		TransactionTreeIterator iter = (TransactionTreeIterator) target
				.iterator();
		while (iter.hasNext()) {
			Pair<IntArrayList, Long> patternPair = iter.next();
			if (patternPair.getFirst().equals(pattern)) {
				assertEquals("The count should be 6", 6, patternPair
						.getSecond().intValue());
			}
		}
	}

//	@Test
	public void testShorterUniquePatterns() {
		IntArrayList shorter = new IntArrayList();
		shorter.add(1);
		shorter.add(3);
		shorter.add(5);
		IntArrayList exact = new IntArrayList();
		exact.add(1);
		exact.add(3);
		exact.add(5);
		exact.add(7);

		int numNodes = target.addPatternUnique(shorter, 6);
		assertEquals("No nodes should have been added", 0, numNodes);

		TransactionTreeIterator iter = (TransactionTreeIterator) target
				.iterator();
		while (iter.hasNext()) {
			Pair<IntArrayList, Long> patternPair = iter.next();
			if (patternPair.getFirst().equals(shorter)) {
				assertEquals("The count should be 3", 3, patternPair
						.getSecond().intValue());
			} else if (patternPair.getFirst().equals(exact)) {
				assertEquals("The count should be 3", 3, patternPair
						.getSecond().intValue());
			}
		}
	}

//	@Test
	public void testLongerUniquePatterns() {
		IntArrayList longer = new IntArrayList();
		longer.add(1);
		longer.add(3);
		longer.add(5);
		longer.add(9);
		longer.add(10);
		IntArrayList exact = new IntArrayList();
		exact.add(1);
		exact.add(3);
		exact.add(5);
		exact.add(7);
		IntArrayList sub = new IntArrayList();
		sub.add(1);
		sub.add(3);
		sub.add(5);

		int numNodes = target.addPatternUnique(longer, 6);
		assertEquals("2 nodes should have been added", 2, numNodes);

		TransactionTreeIterator iter = (TransactionTreeIterator) target
				.iterator();
		while (iter.hasNext()) {
			Pair<IntArrayList, Long> patternPair = iter.next();
			if (patternPair.getFirst().equals(longer)) {
				assertEquals("The count should be 6", 6, patternPair
						.getSecond().intValue());
			} else if (patternPair.getFirst().equals(exact)) {
				assertEquals("The count should be 3", 3, patternPair
						.getSecond().intValue());
			} else if (patternPair.getFirst().equals(sub)) {
				// This will never happen.. is it even a case we care about??
				// TODO
				assertEquals("The count should be 6", 6, patternPair
						.getSecond().intValue());
			}
		}
	}

//	@Test
	public void testClosedIter() {
		IntArrayList longer = new IntArrayList();
		longer.add(1);
		longer.add(3);
		longer.add(5);
		longer.add(7);
		longer.add(9);

		IntArrayList exact = new IntArrayList();
		exact.add(1);
		exact.add(3);
		exact.add(5);
		exact.add(7);
		
		IntArrayList sub = new IntArrayList();
		sub.add(1);
		sub.add(3);
		sub.add(5);

		int numNodes = target.addPattern(longer, 6);
		assertEquals("1 nodes should have been added", 1, numNodes);

		numNodes = target.addPattern(sub, 4);
		assertEquals("0 nodes should have been added", 0, numNodes);

		TransactionTreeIterator iter = (TransactionTreeIterator) target
				.iterator(true);
		while (iter.hasNext()) {
			Pair<IntArrayList, Long> patternPair = iter.next();
			if (patternPair.getFirst().equals(exact)) {
				fail("This should have been overshadowed by the longer");
			} else if (patternPair.getFirst().equals(sub)) {
				fail("This should have been overshadowed by the exact");
			}
		}
	}
}
