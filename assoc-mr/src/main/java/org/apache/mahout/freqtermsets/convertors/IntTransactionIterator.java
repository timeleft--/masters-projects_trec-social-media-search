package org.apache.mahout.freqtermsets.convertors;

import java.util.Iterator;

import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.ParallelFPGrowthReducer.IteratorAdapter;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

public class IntTransactionIterator extends TransactionIterator<Integer> {

	public IntTransactionIterator(Iterator<Pair<IntArrayList, Long>> iterator,
			OpenObjectIntHashMap/*<Integer>*/ attributeIdMapping) {
		super(new IteratorAdapter(iterator), attributeIdMapping);
	}

}
