package ca.uwaterloo.trec2012;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.fpm.pfpgrowth.CountDescendingPairComparator;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextStatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.convertors.SequenceFileOutputCollector;
import org.apache.mahout.fpm.pfpgrowth.convertors.StatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.convertors.TopKPatternsOutputConverter;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.StringOutputConverter;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPTree;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPTreeDepthCache;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FrequentPatternMaxHeap;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.Pattern;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uwaterloo.util.NotifyStream;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

public class LuceneFreqTermSets {
	private static Logger L = LoggerFactory.getLogger("LuceneFreqTermSets");

	public static class FPGrowthCallable implements
			Callable<Map<Integer, FrequentPatternMaxHeap>> {

		final IndexReader ixRd;
		final int sStart;
		final int numDocs;

		final String outPath;
		final int sliceMax;

		public FPGrowthCallable(int s, int numDocs, IndexReader ixRd,
				String outPath) {
			this.sStart = s;
			this.numDocs = numDocs;
			this.ixRd = ixRd;
			this.outPath = outPath;
			this.sliceMax = sStart + (pSampleSize - 1);
		}

		public static List<Pair<String, TopKStringPatterns>> readFrequentPattern(
				Configuration conf, Path path) {
			List<Pair<String, TopKStringPatterns>> ret = Lists.newArrayList();
			// key is feature value is count
			for (Pair<Writable, TopKStringPatterns> record : new SequenceFileIterable<Writable, TopKStringPatterns>(
					path, true, conf)) {
				ret.add(new Pair<String, TopKStringPatterns>(record.getFirst()
						.toString(), new TopKStringPatterns(record.getSecond()
						.getPatterns())));
			}
			return ret;
		}

		/**
		 * Top K FpGrowth Algorithm
		 * 
		 * @param tree
		 *            to be mined
		 * @param minSupportValue
		 *            minimum support of the pattern to keep
		 * @param k
		 *            Number of top frequent patterns to keep
		 * @param requiredFeatures
		 *            Set of integer id's of features to mine
		 * @param outputCollector
		 *            the Collector class which converts the given frequent
		 *            pattern in integer to A
		 * @return Top K Frequent Patterns for each feature and their support
		 */
		private Map<Integer, FrequentPatternMaxHeap> fpGrowth(FPTree tree,
				long minSupportValue, int k,
				Collection<Integer> requiredFeatures,
				TopKPatternsOutputConverter<String> outputCollector,
				StatusUpdater updater) throws IOException {

			Map<Integer, FrequentPatternMaxHeap> patterns = Maps.newHashMap();
			FPTreeDepthCache treeCache = new FPTreeDepthCache();
			for (int i = tree.getHeaderTableCount() - 1; i >= 0; i--) {
				int attribute = tree.getAttributeAtIndex(i);
				if (requiredFeatures.contains(attribute)) {
					L.info("Mining FTree Tree for all patterns with {}",
							attribute);
					MutableLong minSupport = new MutableLong(minSupportValue);
					FrequentPatternMaxHeap frequentPatterns = growth(tree,
							minSupport, k, treeCache, 0, attribute, updater);
					patterns.put(attribute, frequentPatterns);
					outputCollector.collect(attribute, frequentPatterns);

					minSupportValue = Math.max(minSupportValue,
							minSupport.longValue() / 2);
					L.info("Found {} Patterns with Least Support {}", patterns
							.get(attribute).count(), patterns.get(attribute)
							.leastSupport());
				}
			}
			L.info("Tree Cache: First Level: Cache hits={} Cache Misses={}",
					treeCache.getHits(), treeCache.getMisses());
			return patterns;
		}

		private static FrequentPatternMaxHeap generateSinglePathPatterns(
				FPTree tree, int k, long minSupport) {
			FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(
					k, false);

			int tempNode = FPTree.ROOTNODEID;
			Pattern frequentItem = new Pattern();
			while (tree.childCount(tempNode) != 0) {
				if (tree.childCount(tempNode) > 1) {
					L.info("This should not happen {} {}",
							tree.childCount(tempNode), tempNode);
				}
				tempNode = tree.childAtIndex(tempNode, 0);
				if (tree.count(tempNode) >= minSupport) {
					frequentItem.add(tree.attribute(tempNode),
							tree.count(tempNode));
				}
			}
			if (frequentItem.length() > 0) {
				frequentPatterns.insert(frequentItem);
			}

			return frequentPatterns;
		}

		/**
		 * Internal TopKFrequentPattern Generation algorithm, which represents
		 * the A's as integers and transforms features to use only integers
		 * 
		 * @param transactions
		 *            Transaction database Iterator
		 * @param attributeFrequency
		 *            array representing the Frequency of the corresponding
		 *            attribute id
		 * @param minSupport
		 *            minimum support of the pattern to be mined
		 * @param k
		 *            Max value of the Size of the Max-Heap in which Patterns
		 *            are held
		 * @param featureSetSize
		 *            number of features
		 * @param returnFeatures
		 *            the id's of the features for which Top K patterns have to
		 *            be mined
		 * @param topKPatternsOutputCollector
		 *            the outputCollector which transforms the given Pattern in
		 *            integer format to the corresponding A Format
		 * @return Top K frequent patterns for each attribute
		 */
		private Map<Integer, FrequentPatternMaxHeap> generateTopKFrequentPatterns(
				Iterator<Pair<Integer[], Long>> transactions,
				long[] attributeFrequency,
				long minSupport,
				int k,
				int featureSetSize,
				Collection<Integer> returnFeatures,
				TopKPatternsOutputConverter<String> topKPatternsOutputCollector,
				StatusUpdater updater) throws IOException {

			FPTree tree = new FPTree(featureSetSize);
			for (int i = 0; i < featureSetSize; i++) {
				tree.addHeaderCount(i, attributeFrequency[i]);
			}

			// Constructing initial FPTree from the list of transactions
			int nodecount = 0;
			// int attribcount = 0;
			int i = 0;
			while (transactions.hasNext()) {
				Pair<Integer[], Long> transaction = transactions.next();
				Arrays.sort(transaction.getFirst());
				// attribcount += transaction.length;
				nodecount += treeAddCount(tree, transaction.getFirst(),
						transaction.getSecond(), minSupport, attributeFrequency);
				i++;
				if (i % 10000 == 0) {
					L.info("FPTree Building: Read {} Transactions", i);
				}
			}

			L.info("Number of Nodes in the FP Tree: {}", nodecount);

			return fpGrowth(tree, minSupport, k, returnFeatures,
					topKPatternsOutputCollector, updater);
		}

		private static FrequentPatternMaxHeap growth(FPTree tree,
				MutableLong minSupportMutable, int k,
				FPTreeDepthCache treeCache, int level, int currentAttribute,
				StatusUpdater updater) {

			FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(
					k, true);

			int i = Arrays.binarySearch(tree.getHeaderTableAttributes(),
					currentAttribute);
			if (i < 0) {
				return frequentPatterns;
			}

			int headerTableCount = tree.getHeaderTableCount();

			while (i < headerTableCount) {
				int attribute = tree.getAttributeAtIndex(i);
				long count = tree.getHeaderSupportCount(attribute);
				if (count < minSupportMutable.longValue()) {
					i++;
					continue;
				}
				updater.update("FPGrowth Algorithm for a given feature: "
						+ attribute);
				FPTree conditionalTree = treeCache.getFirstLevelTree(attribute);
				if (conditionalTree.isEmpty()) {
					traverseAndBuildConditionalFPTreeData(
							tree.getHeaderNext(attribute),
							minSupportMutable.longValue(), conditionalTree,
							tree);
					// printTree(conditionalTree);

				}

				FrequentPatternMaxHeap returnedPatterns;
				if (attribute == currentAttribute) {

					returnedPatterns = growthTopDown(conditionalTree,
							minSupportMutable, k, treeCache, level + 1, true,
							currentAttribute, updater);

					frequentPatterns = mergeHeap(frequentPatterns,
							returnedPatterns, attribute, count, false); //YA.. too many single attr patterns: true);
				} else {
					returnedPatterns = growthTopDown(conditionalTree,
							minSupportMutable, k, treeCache, level + 1, false,
							currentAttribute, updater);
					frequentPatterns = mergeHeap(frequentPatterns,
							returnedPatterns, attribute, count, false);
				}
				if (frequentPatterns.isFull()
						&& minSupportMutable.longValue() < frequentPatterns
								.leastSupport()) {
					minSupportMutable.setValue(frequentPatterns.leastSupport());
				}
				i++;
			}

			return frequentPatterns;
		}

		private static FrequentPatternMaxHeap growthBottomUp(FPTree tree,
				MutableLong minSupportMutable, int k,
				FPTreeDepthCache treeCache, int level,
				boolean conditionalOfCurrentAttribute, int currentAttribute,
				StatusUpdater updater) {

			FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(
					k, false);

			if (!conditionalOfCurrentAttribute) {
				int index = Arrays.binarySearch(
						tree.getHeaderTableAttributes(), currentAttribute);
				if (index < 0) {
					return frequentPatterns;
				} else {
					int attribute = tree.getAttributeAtIndex(index);
					long count = tree.getHeaderSupportCount(attribute);
					if (count < minSupportMutable.longValue()) {
						return frequentPatterns;
					}
				}
			}

			if (tree.singlePath()) {
				return generateSinglePathPatterns(tree, k,
						minSupportMutable.longValue());
			}

			updater.update("Bottom Up FP Growth");
			for (int i = tree.getHeaderTableCount() - 1; i >= 0; i--) {
				int attribute = tree.getAttributeAtIndex(i);
				long count = tree.getHeaderSupportCount(attribute);
				if (count < minSupportMutable.longValue()) {
					continue;
				}
				FPTree conditionalTree = treeCache.getTree(level);

				FrequentPatternMaxHeap returnedPatterns;
				if (conditionalOfCurrentAttribute) {
					traverseAndBuildConditionalFPTreeData(
							tree.getHeaderNext(attribute),
							minSupportMutable.longValue(), conditionalTree,
							tree);
					returnedPatterns = growthBottomUp(conditionalTree,
							minSupportMutable, k, treeCache, level + 1, true,
							currentAttribute, updater);

					frequentPatterns = mergeHeap(frequentPatterns,
							returnedPatterns, attribute, count, true);
				} else {
					if (attribute == currentAttribute) {
						traverseAndBuildConditionalFPTreeData(
								tree.getHeaderNext(attribute),
								minSupportMutable.longValue(), conditionalTree,
								tree);
						returnedPatterns = growthBottomUp(conditionalTree,
								minSupportMutable, k, treeCache, level + 1,
								true, currentAttribute, updater);

						frequentPatterns = mergeHeap(frequentPatterns,
								returnedPatterns, attribute, count, true);
					} else if (attribute > currentAttribute) {
						traverseAndBuildConditionalFPTreeData(
								tree.getHeaderNext(attribute),
								minSupportMutable.longValue(), conditionalTree,
								tree);
						returnedPatterns = growthBottomUp(conditionalTree,
								minSupportMutable, k, treeCache, level + 1,
								false, currentAttribute, updater);
						frequentPatterns = mergeHeap(frequentPatterns,
								returnedPatterns, attribute, count, false);
					}
				}

				if (frequentPatterns.isFull()
						&& minSupportMutable.longValue() < frequentPatterns
								.leastSupport()) {
					minSupportMutable.setValue(frequentPatterns.leastSupport());
				}
			}

			return frequentPatterns;
		}

		private static FrequentPatternMaxHeap growthTopDown(FPTree tree,
				MutableLong minSupportMutable, int k,
				FPTreeDepthCache treeCache, int level,
				boolean conditionalOfCurrentAttribute, int currentAttribute,
				StatusUpdater updater) {

			FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(
					k, true);

			if (!conditionalOfCurrentAttribute) {
				int index = Arrays.binarySearch(
						tree.getHeaderTableAttributes(), currentAttribute);
				if (index < 0) {
					return frequentPatterns;
				} else {
					int attribute = tree.getAttributeAtIndex(index);
					long count = tree.getHeaderSupportCount(attribute);
					if (count < minSupportMutable.longValue()) {
						return frequentPatterns;
					}
				}
			}

			if (tree.singlePath()) {
				return generateSinglePathPatterns(tree, k,
						minSupportMutable.longValue());
			}

			updater.update("Top Down Growth:");

			for (int i = 0; i < tree.getHeaderTableCount(); i++) {
				int attribute = tree.getAttributeAtIndex(i);
				long count = tree.getHeaderSupportCount(attribute);
				if (count < minSupportMutable.longValue()) {
					continue;
				}

				FPTree conditionalTree = treeCache.getTree(level);

				FrequentPatternMaxHeap returnedPatterns;
				if (conditionalOfCurrentAttribute) {
					traverseAndBuildConditionalFPTreeData(
							tree.getHeaderNext(attribute),
							minSupportMutable.longValue(), conditionalTree,
							tree);

					returnedPatterns = growthBottomUp(conditionalTree,
							minSupportMutable, k, treeCache, level + 1, true,
							currentAttribute, updater);
					frequentPatterns = mergeHeap(frequentPatterns,
							returnedPatterns, attribute, count, true);

				} else {
					if (attribute == currentAttribute) {
						traverseAndBuildConditionalFPTreeData(
								tree.getHeaderNext(attribute),
								minSupportMutable.longValue(), conditionalTree,
								tree);
						returnedPatterns = growthBottomUp(conditionalTree,
								minSupportMutable, k, treeCache, level + 1,
								true, currentAttribute, updater);
						frequentPatterns = mergeHeap(frequentPatterns,
								returnedPatterns, attribute, count, true);

					} else if (attribute > currentAttribute) {
						traverseAndBuildConditionalFPTreeData(
								tree.getHeaderNext(attribute),
								minSupportMutable.longValue(), conditionalTree,
								tree);
						returnedPatterns = growthBottomUp(conditionalTree,
								minSupportMutable, k, treeCache, level + 1,
								false, currentAttribute, updater);
						frequentPatterns = mergeHeap(frequentPatterns,
								returnedPatterns, attribute, count, false);

					}
				}
				if (frequentPatterns.isFull()
						&& minSupportMutable.longValue() < frequentPatterns
								.leastSupport()) {
					minSupportMutable.setValue(frequentPatterns.leastSupport());
				}
			}

			return frequentPatterns;
		}
		static  TextCategorizer langCat = new TextCategorizer();
		private static FrequentPatternMaxHeap mergeHeap(
				FrequentPatternMaxHeap frequentPatterns,
				FrequentPatternMaxHeap returnedPatterns, int attribute,
				long count, boolean addAttribute) {
//			frequentPatterns.addAll(returnedPatterns, attribute, count);
			for (Pattern pattern : returnedPatterns.getHeap()) {
				if(pattern.length() > 1){
					StringBuilder txt = new StringBuilder();
					for(int p: pattern.getPattern()){
						String pTxt = mIdTermMap.get(p);
						char pCh0 = pTxt.charAt(0);
						if(pCh0 == '@' ||pCh0 == '#'){
							pTxt = pTxt.substring(1);
						}
						txt.append(pTxt).append(" ");
					}
					if(!"english".equals(langCat.categorize(txt.toString()))){
						L.warn("Prunning non-english pattern: {}", txt);
						continue;
					}
				}
				long support = Math.min(count, pattern.support());
				if (frequentPatterns.addable(support)) {
					pattern.add(attribute, support);
					frequentPatterns.insert(pattern);
				}
			}
			if (frequentPatterns.addable(count) && addAttribute) {
				Pattern p = new Pattern();
				p.add(attribute, count);
				frequentPatterns.insert(p);
			}

			return frequentPatterns;
		}

		private static void traverseAndBuildConditionalFPTreeData(
				int firstConditionalNode, long minSupport,
				FPTree conditionalTree, FPTree tree) {

			// Build Subtable
			int conditionalNode = firstConditionalNode;

			while (conditionalNode != -1) {
				long nextNodeCount = tree.count(conditionalNode);
				int pathNode = tree.parent(conditionalNode);
				int prevConditional = -1;

				while (pathNode != 0) { // dummy root node
					int attribute = tree.attribute(pathNode);
					if (tree.getHeaderSupportCount(attribute) < minSupport) {
						pathNode = tree.parent(pathNode);
						continue;
					}
					// update and increment the headerTable Counts
					conditionalTree.addHeaderCount(attribute, nextNodeCount);

					int conditional = tree.conditional(pathNode);
					// if its a new conditional tree node

					if (conditional == 0) {
						tree.setConditional(pathNode, conditionalTree
								.createConditionalNode(attribute, 0));
						conditional = tree.conditional(pathNode);
						conditionalTree.addHeaderNext(attribute, conditional);
					} else {
						conditionalTree.setSinglePath(false);
					}

					if (prevConditional != -1) { // if there is a child element
						int prevParent = conditionalTree
								.parent(prevConditional);
						if (prevParent == -1) {
							conditionalTree.setParent(prevConditional,
									conditional);
						} else if (prevParent != conditional) {
							throw new IllegalStateException();
						}
					}

					conditionalTree.addCount(conditional, nextNodeCount);
					prevConditional = conditional;

					pathNode = tree.parent(pathNode);

				}

				if (prevConditional != -1) {
					int prevParent = conditionalTree.parent(prevConditional);
					if (prevParent == -1) {
						conditionalTree.setParent(prevConditional,
								FPTree.ROOTNODEID);
					} else if (prevParent != FPTree.ROOTNODEID) {
						throw new IllegalStateException();
					}
					if (conditionalTree.childCount(FPTree.ROOTNODEID) > 1
							&& conditionalTree.singlePath()) {
						conditionalTree.setSinglePath(false);
					}
				}
				conditionalNode = tree.next(conditionalNode);
			}

			tree.clearConditional();
			conditionalTree.reorderHeaderTable();
			pruneFPTree(minSupport, conditionalTree);
			// prune Conditional Tree

		}

		private static void pruneFPTree(long minSupport, FPTree tree) {
			for (int i = 0; i < tree.getHeaderTableCount(); i++) {
				int currentAttribute = tree.getAttributeAtIndex(i);
				if (tree.getHeaderSupportCount(currentAttribute) < minSupport) {
					int nextNode = tree.getHeaderNext(currentAttribute);
					tree.removeHeaderNext(currentAttribute);
					while (nextNode != -1) {

						int mychildCount = tree.childCount(nextNode);

						int parentNode = tree.parent(nextNode);

						for (int j = 0; j < mychildCount; j++) {
							Integer myChildId = tree.childAtIndex(nextNode, j);
							tree.replaceChild(parentNode, nextNode, myChildId);
						}
						nextNode = tree.next(nextNode);
					}

				}
			}

			for (int i = 0; i < tree.getHeaderTableCount(); i++) {
				int currentAttribute = tree.getAttributeAtIndex(i);
				int nextNode = tree.getHeaderNext(currentAttribute);

				OpenIntIntHashMap prevNode = new OpenIntIntHashMap();
				int justPrevNode = -1;
				while (nextNode != -1) {

					int parent = tree.parent(nextNode);

					if (prevNode.containsKey(parent)) {
						int prevNodeId = prevNode.get(parent);
						if (tree.childCount(prevNodeId) <= 1
								&& tree.childCount(nextNode) <= 1) {
							tree.addCount(prevNodeId, tree.count(nextNode));
							tree.addCount(nextNode, -1 * tree.count(nextNode));
							if (tree.childCount(nextNode) == 1) {
								tree.addChild(prevNodeId,
										tree.childAtIndex(nextNode, 0));
								tree.setParent(tree.childAtIndex(nextNode, 0),
										prevNodeId);
							}
							tree.setNext(justPrevNode, tree.next(nextNode));
						}
					} else {
						prevNode.put(parent, nextNode);
					}
					justPrevNode = nextNode;
					nextNode = tree.next(nextNode);
				}
			}

			// prune Conditional Tree

		}

		/**
		 * Create FPTree with node counts incremented by addCount variable given
		 * the root node and the List of Attributes in transaction sorted by
		 * support
		 * 
		 * @param tree
		 *            object to which the transaction has to be added to
		 * @param myList
		 *            List of transactions sorted by support
		 * @param addCount
		 *            amount by which the Node count has to be incremented
		 * @param minSupport
		 *            the MutableLong value which contains the current
		 *            value(dynamic) of support
		 * @param attributeFrequency
		 *            the list of attributes and their frequency
		 * @return the number of new nodes added
		 */
		private static int treeAddCount(FPTree tree, Integer[] myList,
				long addCount, long minSupport, long[] attributeFrequency) {

			int temp = FPTree.ROOTNODEID;
			int ret = 0;
			boolean addCountMode = true;

			for (int attribute : myList) {
				if (attributeFrequency[attribute] < minSupport) {
					return ret;
				}
				int child;
				if (addCountMode) {
					child = tree.childWithAttribute(temp, attribute);
					if (child == -1) {
						addCountMode = false;
					} else {
						tree.addCount(child, addCount);
						temp = child;
					}
				}
				if (!addCountMode) {
					child = tree.createNode(temp, attribute, addCount);
					temp = child;
					ret++;
				}
			}

			return ret;

		}

		public Map<Integer, FrequentPatternMaxHeap> call() throws Exception {

			@SuppressWarnings("unchecked")
			List<Integer>[] slice = new List[pSampleSize];
			int i = 0;
			for (; i < pSampleSize && (sStart + i < numDocs); ++i) {
				slice[i] = Lists.<Integer> newLinkedList();
			}
//			double samplePropotion = 1.0 * i / numDocs;
			long minSupport = pMinSupport; 
//					Math.max(pMinSupport, 1 + Math.round(pMinDocFreq * samplePropotion));

			Map<String, Integer> termIdMap = Maps
					.<String, Integer> newHashMap();
			List<Pair<String, Long>> fList = Lists
					.<Pair<String, Long>> newArrayList();

			TermIterator tIter = new TermIterator(ixRd, pFieldName,
					pMinDocFreq, pMaxDocPct);
			int tNum = 0;
			while (tIter.hasNext()) {
				Term t = tIter.next();

				TermDocs tDocs = ixRd.termDocs(t);

				tDocs.skipTo(sStart);
				int stDocs = 0;
				do {
					int d = tDocs.doc();
					if (d > sliceMax)
						break;

					++stDocs;
					List<Integer> inst = slice[(d % pSampleSize)];
					// if (pUseBooleanFeats) {
					inst.add(tNum);
					// } else {
					// TermFreqVector tfv = ixRd.getTermFreqVector(d,
					// pFieldName);
					// inst[tNum] =
					// tfv.getTermFrequencies()[tfv.indexOf(t.text())];
					// }

				} while (tDocs.next());

				if (stDocs > 0) {
					fList.add(new Pair<String, Long>(t.text(), Long.valueOf(stDocs)));
//							Math.max(pMinSupport, 1 + Math
//							.round(ixRd.docFreq(t) * samplePropotion))));
				}

				termIdMap.put(t.text(), tNum);

				++tNum;
				if (tNum % 1000 == 0) {
					L.info(String.format(
							"Added %d tokens to matrix of slice starting %d",
							tNum, sStart));
				}
			}

			Collections.sort(fList,
					new CountDescendingPairComparator<String, Long>());

			long[] attributeFrequency = new long[mIdTermMap.size()];
			for (Pair<String, Long> feature : fList) {
				String attrib = feature.getFirst();
				Long frequency = feature.getSecond();
				if (frequency < minSupport) { // TODONE: Yes --> should this be
												// pMinSupport (proportion)
					break; // Will never happen!! just copy pasted as is!
				}
				attributeFrequency[termIdMap.get(attrib)] = frequency;
			}

			termIdMap = null;
			L.info("Number of unique items {}", fList.size());

			Iterator<Pair<Integer[], Long>> sliceIter = Iterators.transform(
					Iterators.forArray(slice),
					new Function<List<Integer>, Pair<Integer[], Long>>() {
						public Pair<Integer[], Long> apply(List<Integer> input) {
							Integer[] a = new Integer[input.size()];
							input.toArray(a);
							// TODO: Is this the right value (1L) or should we
							// use doc length or whatever?
							return new Pair<Integer[], Long>(a, 1L);
						}
					});

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(outPath, "trec2011-twitter_" + sStart + "-"
					+ sliceMax + "freq-terms");
			if (fs.exists(path)) {
				L.warn("Deleting: " + path + " already exists!");
				fs.delete(path, false);
			}
			SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
					path, Text.class, TopKStringPatterns.class);
			Map<Integer, FrequentPatternMaxHeap> result;
			try {
				StringOutputConverter output = new StringOutputConverter(
						new SequenceFileOutputCollector<Text, TopKStringPatterns>(
								writer));

				result = generateTopKFrequentPatterns(sliceIter,
						attributeFrequency, minSupport, pMaxHeapSize,
						mIdTermMap.size(), mIdTermMap.keySet(),
						new TopKPatternsOutputConverter<String>(output,
								mIdTermMap), new ContextStatusUpdater(null));
			} finally {
				Closeables.closeQuietly(writer);
			}
			return result;
		}

	}

	static String pIxPath = "D:\\datasets\\twitter-trec2011\\index";
	static String pOutPath = "D:\\datasets\\twitter-trec2011\\freq-terms_supp5-k25000";
	
	static int pSampleSize = (int) 1E4;
	static int pMinDocFreq = 100;
	static int pMaxDocPct = 95;
	
	static String pFieldName = "text";
	static boolean pUseBooleanFeats = false;
	static int pNumThreads = 2;
	
	static int pMaxHeapSize = 10000;
	static int pMinSupport = 10;

	static Map<Integer, String> mIdTermMap = Maps
			.<Integer, String> newConcurrentMap();

	// static Set<String> mReturnableFeats = Sets.<String>newHashSet();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig,
				"LuceneFreqTermSets");
		try {
			System.setErr(new PrintStream(notifyStream));

			File ixFile = new File(pIxPath);
			Directory ixDir = new MMapDirectory(ixFile);
			final IndexReader ixRd = IndexReader.open(ixDir);
			pMaxDocPct = pMaxDocPct * ixRd.numDocs() / 100;
			// pMinSupport = pMinDocFreq * pSampleSize / ixRd.numDocs();
			try {
				L.info("Opened index: " + pIxPath);

				// List<Pair<String,Long>> freqList =
				// Lists.<Pair<String,Long>>newLinkedList();

				TermIterator tIter = new TermIterator(ixRd, pFieldName,
						pMinDocFreq, pMaxDocPct);
				int tId = 0;
				while (tIter.hasNext()) {
					Term t = tIter.next();
					// // if (pUseBooleanFeats) {
					// // docFreqList.add(new Pair<String, Long>(t.text(), 1L));
					// // } else {
					// freqList.add(new Pair<String, Long>(t.text(), new
					// Long(ixRd.docFreq(t))));
					// // }
					// mReturnableFeats.add(t.text());
					mIdTermMap.put(tId, t.text());
					++tId;
					if (tId % 1000 == 0) {
						L.info(String.format(
								"Added %d tokens to attribute vector", tId));
					}
				}

				L.info("Creating ARFF dataset with num attributes: " + tId);
				// int numTerms = tVector.size();
				int numDocs = ixRd.numDocs();

				ExecutorService exec = Executors
						.newFixedThreadPool(pNumThreads);
				Future<Map<Integer, FrequentPatternMaxHeap>> lastFuture = null;

				for (int s = 1; s < numDocs; s += pSampleSize) {
					FPGrowthCallable sTrans = new FPGrowthCallable(s, numDocs,
							ixRd, pOutPath);

					lastFuture = exec.submit(sTrans);
					// Instances sInsts = sTrans.call();
					// Enumeration instEnum = sInsts.enumerateInstances();
					// while (instEnum.hasMoreElements()) {
					// Instance inst = (Instance) instEnum.nextElement();
					// dataset.add(inst);
					// inst.setDataset(dataset);
					// }
				}

				lastFuture.get();

				exec.shutdown();
				while (!exec.isTerminated()) {
					Thread.sleep(1000);
				}

			} finally {
				ixRd.close();
			}
		} catch (Exception ex) {
			L.error(ex.getMessage(), ex);
		} finally {
			try {
				notifyStream.flush();
				notifyStream.close();
			} catch (IOException ignored) {
				L.error(ignored.getMessage(), ignored);
			}

			System.setErr(errOrig);
		}
	}

	public static class TermIterator extends AbstractIterator<Term> {
		TermEnum tEnum;
		IndexReader ixRd;
		String fieldName;
		int minDocFreq;
		int maxDocPct;
		static HashSet<String> stopWords = new HashSet<String>();
		static{
			stopWords.addAll(Arrays.asList(
					"a",
					"a's",
					"able",
					"about",
					"above",
					"according",
					"accordingly",
					"across",
					"actually",
					"after",
					"afterwards",
					"again",
					"against",
					"ain't",
					"all",
					"allow",
					"allows",
					"almost",
					"alone",
					"along",
					"already",
					"also",
					"although",
					"always",
					"am",
					"among",
					"amongst",
					"an",
					"and",
					"another",
					"any",
					"anybody",
					"anyhow",
					"anyone",
					"anything",
					"anyway",
					"anyways",
					"anywhere",
					"apart",
					"appear",
					"appreciate",
					"appropriate",
					"are",
					"aren't",
					"around",
					"as",
					"aside",
					"ask",
					"asking",
					"associated",
					"at",
					"available",
					"away",
					"awfully",
					"b",
					"be",
					"became",
					"because",
					"become",
					"becomes",
					"becoming",
					"been",
					"before",
					"beforehand",
					"behind",
					"being",
					"believe",
					"below",
					"beside",
					"besides",
					"best",
					"better",
					"between",
					"beyond",
					"both",
					"brief",
					"but",
					"by",
					"c",
					"c'mon",
					"c's",
					"came",
					"can",
					"can't",
					"cannot",
					"cant",
					"cause",
					"causes",
					"certain",
					"certainly",
					"changes",
					"clearly",
					"co",
					"com",
					"come",
					"comes",
					"concerning",
					"consequently",
					"consider",
					"considering",
					"contain",
					"containing",
					"contains",
					"corresponding",
					"could",
					"couldn't",
					"course",
					"currently",
					"d",
					"definitely",
					"described",
					"despite",
					"did",
					"didn't",
					"different",
					"do",
					"does",
					"doesn't",
					"doing",
					"don't",
					"done",
					"down",
					"downwards",
					"during",
					"e",
					"each",
					"edu",
					"eg",
					"eight",
					"either",
					"else",
					"elsewhere",
					"enough",
					"entirely",
					"especially",
					"et",
					"etc",
					"even",
					"ever",
					"every",
					"everybody",
					"everyone",
					"everything",
					"everywhere",
					"ex",
					"exactly",
					"example",
					"except",
					"f",
					"far",
					"few",
					"fifth",
					"first",
					"five",
					"followed",
					"following",
					"follows",
					"for",
					"former",
					"formerly",
					"forth",
					"four",
					"from",
					"further",
					"furthermore",
					"g",
					"get",
					"gets",
					"getting",
					"given",
					"gives",
					"go",
					"goes",
					"going",
					"gone",
					"got",
					"gotten",
					"greetings",
					"h",
					"had",
					"hadn't",
					"happens",
					"hardly",
					"has",
					"hasn't",
					"have",
					"haven't",
					"having",
					"he",
					"he's",
					"hello",
					"help",
					"hence",
					"her",
					"here",
					"here's",
					"hereafter",
					"hereby",
					"herein",
					"hereupon",
					"hers",
					"herself",
					"hi",
					"him",
					"himself",
					"his",
					"hither",
					"hopefully",
					"how",
					"howbeit",
					"however",
					"i",
					"i'd",
					"i'll",
					"i'm",
					"i've",
					"ie",
					"if",
					"ignored",
					"immediate",
					"in",
					"inasmuch",
					"inc",
					"indeed",
					"indicate",
					"indicated",
					"indicates",
					"inner",
					"insofar",
					"instead",
					"into",
					"inward",
					"is",
					"isn't",
					"it",
					"it'd",
					"it'll",
					"it's",
					"its",
					"itself",
					"j",
					"just",
					"k",
					"keep",
					"keeps",
					"kept",
					"know",
					"knows",
					"known",
					"l",
					"last",
					"lately",
					"later",
					"latter",
					"latterly",
					"least",
					"less",
					"lest",
					"let",
					"let's",
					"like",
					"liked",
					"likely",
					"little",
					"look",
					"looking",
					"looks",
					"ltd",
					"m",
					"mainly",
					"many",
					"may",
					"maybe",
					"me",
					"mean",
					"meanwhile",
					"merely",
					"might",
					"more",
					"moreover",
					"most",
					"mostly",
					"much",
					"must",
					"my",
					"myself",
					"n",
					"name",
					"namely",
					"nd",
					"near",
					"nearly",
					"necessary",
					"need",
					"needs",
					"neither",
					"never",
					"nevertheless",
					"new",
					"next",
					"nine",
					"no",
					"nobody",
					"non",
					"none",
					"noone",
					"nor",
					"normally",
					"not",
					"nothing",
					"novel",
					"now",
					"nowhere",
					"o",
					"obviously",
					"of",
					"off",
					"often",
					"oh",
					"ok",
					"okay",
					"old",
					"on",
					"once",
					"one",
					"ones",
					"only",
					"onto",
					"or",
					"other",
					"others",
					"otherwise",
					"ought",
					"our",
					"ours",
					"ourselves",
					"out",
					"outside",
					"over",
					"overall",
					"own",
					"p",
					"particular",
					"particularly",
					"per",
					"perhaps",
					"placed",
					"please",
					"plus",
					"possible",
					"presumably",
					"probably",
					"provides",
					"q",
					"que",
					"quite",
					"qv",
					"r",
					"rather",
					"rd",
					"re",
					"really",
					"reasonably",
					"regarding",
					"regardless",
					"regards",
					"relatively",
					"respectively",
					"right",
					"s",
					"said",
					"same",
					"saw",
					"say",
					"saying",
					"says",
					"second",
					"secondly",
					"see",
					"seeing",
					"seem",
					"seemed",
					"seeming",
					"seems",
					"seen",
					"self",
					"selves",
					"sensible",
					"sent",
					"serious",
					"seriously",
					"seven",
					"several",
					"shall",
					"she",
					"should",
					"shouldn't",
					"since",
					"six",
					"so",
					"some",
					"somebody",
					"somehow",
					"someone",
					"something",
					"sometime",
					"sometimes",
					"somewhat",
					"somewhere",
					"soon",
					"sorry",
					"specified",
					"specify",
					"specifying",
					"still",
					"sub",
					"such",
					"sup",
					"sure",
					"t",
					"t's",
					"take",
					"taken",
					"tell",
					"tends",
					"th",
					"than",
					"thank",
					"thanks",
					"thanx",
					"that",
					"that's",
					"thats",
					"the",
					"their",
					"theirs",
					"them",
					"themselves",
					"then",
					"thence",
					"there",
					"there's",
					"thereafter",
					"thereby",
					"therefore",
					"therein",
					"theres",
					"thereupon",
					"these",
					"they",
					"they'd",
					"they'll",
					"they're",
					"they've",
					"think",
					"third",
					"this",
					"thorough",
					"thoroughly",
					"those",
					"though",
					"three",
					"through",
					"throughout",
					"thru",
					"thus",
					"to",
					"together",
					"too",
					"took",
					"toward",
					"towards",
					"tried",
					"tries",
					"truly",
					"try",
					"trying",
					"twice",
					"two",
					"u",
					"un",
					"under",
					"unfortunately",
					"unless",
					"unlikely",
					"until",
					"unto",
					"up",
					"upon",
					"us",
					"use",
					"used",
					"useful",
					"uses",
					"using",
					"usually",
					"uucp",
					"v",
					"value",
					"various",
					"very",
					"via",
					"viz",
					"vs",
					"w",
					"want",
					"wants",
					"was",
					"wasn't",
					"way",
					"we",
					"we'd",
					"we'll",
					"we're",
					"we've",
					"welcome",
					"well",
					"went",
					"were",
					"weren't",
					"what",
					"what's",
					"whatever",
					"when",
					"whence",
					"whenever",
					"where",
					"where's",
					"whereafter",
					"whereas",
					"whereby",
					"wherein",
					"whereupon",
					"wherever",
					"whether",
					"which",
					"while",
					"whither",
					"who",
					"who's",
					"whoever",
					"whole",
					"whom",
					"whose",
					"why",
					"will",
					"willing",
					"wish",
					"with",
					"within",
					"without",
					"won't",
					"wonder",
					"would",
					"would",
					"wouldn't",
					"x",
					"y",
					"yes",
					"yet",
					"you",
					"you'd",
					"you'll",
					"you're",
					"you've",
					"your",
					"yours",
					"yourself",
					"yourselves",
					"z",
					"zero",
					"http",
					"www",
					"com"
					));
		}
		
		public TermIterator(IndexReader ixRd, String fieldName, int minDocFreq,
				int maxDocPct) throws IOException {
			this.ixRd = ixRd;
			this.fieldName = fieldName;
			this.minDocFreq = minDocFreq;
			this.maxDocPct = maxDocPct;

			this.tEnum = ixRd.terms(new Term(fieldName, ""));
			// TermEnum tEnum = ixRd.terms();
		}

		@Override
		protected Term computeNext() {
			try {
				while (tEnum.next()) {
					Term t = tEnum.term();
					if (t == null || !(t.field().equals(fieldName))) {
						break;
					}
					int df = ixRd.docFreq(t);
					if (df < minDocFreq || df > maxDocPct) {
						continue;
					}
					String txt = t.text();
					if(txt.length() < 3){
						continue;
					}
					if(stopWords.contains(txt)){
						continue;
					}
					char ch0 = txt.charAt(0);
					if(ch0 > '\u007F' //not basic latin
//					'\u024F' //not latin extended
//					 && !(ch0 >= '\u0600' //not arabic
//					 && ch0 <= '\u06FF') 
					 ){
						continue;
					}
					return t;
				}
			} catch (IOException e) {
				L.error(e.getMessage(), e);
				throw new RuntimeException(e);
			}

			return endOfData();
		}

		// FIXME: Closeables.closeQuietly(tEnum)
	}
}
