/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.freqtermsets.fpgrowth;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ChiSquaredDistributionImpl;
import org.apache.commons.math.distribution.TDistributionImpl;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.freqtermsets.CountDescendingPairComparator;
import org.apache.mahout.freqtermsets.PFPGrowth;
import org.apache.mahout.freqtermsets.TransactionTree;
import org.apache.mahout.freqtermsets.convertors.IntTransactionIterator;
import org.apache.mahout.freqtermsets.convertors.StatusUpdater;
import org.apache.mahout.freqtermsets.convertors.TopKPatternsOutputConverter;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Implementation of PFGrowth Algorithm with FP-Bonsai pruning YA: The Bonsai
 * pruning is really questionable.. i don't think it worked and there are enough
 * patches adding and removing things related to it that I think it is now not
 * part of the algorithm.
 * 
 * @param <A>
 *            object type used as the cell items in a transaction list
 */
public class FPGrowth<A extends Integer> {// Comparable<? super A>> {

	private static final Logger log = LoggerFactory.getLogger(FPGrowth.class);
	private static final float LEAST_NUM_CHILDREN_TO_VOTE_FOR_NOISE = 2;
	private static final double SIGNIFICANCE = 0.05;

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
	 * Generate the Feature Frequency list from the given transaction whose
	 * frequency > minSupport
	 * 
	 * @param transactions
	 *            Iterator over the transaction database
	 * @param minSupport
	 *            minSupport of the feature to be included
	 * @return the List of features and their associated frequency as a Pair
	 */
	public final List<Pair<A, Long>> generateFList(
			Iterator<Pair<List<A>, Long>> transactions, int minSupport) {

		Map<A, MutableLong> attributeSupport = Maps.newHashMap();
		while (transactions.hasNext()) {
			Pair<List<A>, Long> transaction = transactions.next();
			for (A attribute : transaction.getFirst()) {
				if (attributeSupport.containsKey(attribute)) {
					attributeSupport.get(attribute).add(
							transaction.getSecond().longValue());
				} else {
					attributeSupport.put(attribute,
							new MutableLong(transaction.getSecond()));
				}
			}
		}
		List<Pair<A, Long>> fList = Lists.newArrayList();
		for (Entry<A, MutableLong> e : attributeSupport.entrySet()) {
			long value = e.getValue().longValue();
			if (value >= minSupport) {
				fList.add(new Pair<A, Long>(e.getKey(), value));
			}
		}

		Collections.sort(fList, new CountDescendingPairComparator<A, Long>());

		return fList;
	}

	/**
	 * Generate Top K Frequent Patterns for every feature in returnableFeatures
	 * given a stream of transactions and the minimum support
	 * 
	 * @param transactionStream
	 *            Iterator of transaction
	 * @param frequencyList
	 *            list of frequent features and their support value
	 * @param minSupport
	 *            minimum support of the transactions
	 * @param k
	 *            Number of top frequent patterns to keep
	 * @param returnableFeatures
	 *            set of features for which the frequent patterns are mined. If
	 *            the set is empty or null, then top K patterns for every
	 *            frequent item (an item whose support> minSupport) is generated
	 * @param output
	 *            The output collector to which the the generated patterns are
	 *            written
	 * @param numGroups
	 * @param groupId
	 * @throws IOException
	 * @throws MathException
	 */
	public final void generateTopKFrequentPatterns(
			// Iterator<Pair<List<A>, Long>> transactionStream,
			TransactionTree cTree, Collection<Pair<A, Long>> frequencyList,
			long minSupport, int k, Collection<A> returnableFeatures,
			OutputCollector<A, List<Pair<List<A>, Long>>> output,
			StatusUpdater updater, int groupId, int numGroups)
			throws IOException, MathException {

		OpenIntObjectHashMap<A> reverseMapping = new OpenIntObjectHashMap<A>();
		OpenObjectIntHashMap<A> attributeIdMapping = new OpenObjectIntHashMap<A>();

		int id = 0;
		for (Pair<A, Long> feature : frequencyList) {
			A attrib = feature.getFirst();
			Long frequency = feature.getSecond();
			if (frequency >= minSupport) {
				attributeIdMapping.put(attrib, id);
				reverseMapping.put(id++, attrib);
			}
		}

		long[] attributeFrequency = new long[attributeIdMapping.size()];
		for (Pair<A, Long> feature : frequencyList) {
			A attrib = feature.getFirst();
			Long frequency = feature.getSecond();
			if (frequency < minSupport) {
				break;
			}
			attributeFrequency[attributeIdMapping.get(attrib)] = frequency;
		}

		log.info("Number of unique items {}", frequencyList.size());

		Collection<Integer> returnFeatures = new HashSet<Integer>();
		if (returnableFeatures != null && !returnableFeatures.isEmpty()) {
			for (A attrib : returnableFeatures) {
				if (attributeIdMapping.containsKey(attrib)) {
					returnFeatures.add(attributeIdMapping.get(attrib));
					log.info("Adding Pattern {}=>{}", attrib,
							attributeIdMapping.get(attrib));
				}
			}
		} else {
			// YA: Streaming conistent group assignment
			// if(Integer.class.isAssignableFrom(A)){
			for (A attrib : attributeIdMapping.keys()) {
				if (PFPGrowth.isGroupMember(groupId, attrib, numGroups)) {
					returnFeatures.add(attributeIdMapping.get(attrib));
					log.info("Adding Pattern {}=>{}", attrib,
							attributeIdMapping.get(attrib));
				}
			}
			// }
		}

		log.info("Number of unique pruned items {}", attributeIdMapping.size());
		log.info("Number of returnable features {} in group {}",
				returnFeatures.size(), groupId);

		generateTopKFrequentPatterns(cTree,
				attributeIdMapping,
				// new TransactionIterator<A>(
				// transactionStream, attributeIdMapping),
				attributeFrequency, minSupport, k, reverseMapping.size(),
				returnFeatures, new TopKPatternsOutputConverter<A>(output,
						reverseMapping), updater);

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
	 *            the Collector class which converts the given frequent pattern
	 *            in integer to A
	 */
	private void fpGrowth(FPTree tree, long minSupportValue, int k,
			Collection<Integer> requiredFeatures,
			TopKPatternsOutputConverter<A> outputCollector,
			StatusUpdater updater) throws IOException {
		// Map<String, Map<Integer, FrequentPatternMaxHeap>> result =
		// Maps.newHashMap();
		// Map<Integer,FrequentPatternMaxHeap> patterns = Maps.newHashMap();

		FPTreeDepthCache treeCache = new FPTreeDepthCache();
		for (int i = tree.getHeaderTableCount() - 1; i >= 0; i--) {
			int attribute = tree.getAttributeAtIndex(i);
			if (requiredFeatures.contains(attribute)) {
				log.info("Mining FTree Tree for all patterns with '{}'",
						attribute);
				MutableLong minSupport = new MutableLong(minSupportValue);
				FrequentPatternMaxHeap frequentPatterns = growth(tree,
						minSupport, k, treeCache, 0, attribute, updater);

				// Map<Integer, FrequentPatternMaxHeap> patterns =
				// result.get(langSure);
				//
				// if (patterns == null) {
				// patterns = Maps.newHashMap();
				// result.put(langSure, patterns);
				// }
				//
				// patterns.put(attribute, frequentPatterns);
				outputCollector.collect(attribute, frequentPatterns);

				minSupportValue = Math.max(minSupportValue,
						minSupport.longValue() / 2);
				log.info("Found {} Patterns with Least Support {}",
						frequentPatterns.count(),
						frequentPatterns.leastSupport());
			}
		}
		log.info("Tree Cache: First Level: Cache hits={} Cache Misses={}",
				treeCache.getHits(), treeCache.getMisses());
		// return patterns;
		// return result;
	}

	private static FrequentPatternMaxHeap generateSinglePathPatterns(
			FPTree tree, int k, long minSupport) {
		FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(k,
				false);

		int tempNode = FPTree.ROOTNODEID;
		Pattern frequentItem = new Pattern();
		while (tree.childCount(tempNode) != 0) {
			if (tree.childCount(tempNode) > 1) {
				log.info("This should not happen {} {}",
						tree.childCount(tempNode), tempNode);
			}
			tempNode = tree.childAtIndex(tempNode, 0);
			if (tree.count(tempNode) >= minSupport) {
				frequentItem
						.add(tree.attribute(tempNode), tree.count(tempNode));
			}
		}
		if (frequentItem.length() > 0) {
			frequentPatterns.insert(frequentItem);
		}

		return frequentPatterns;
	}

	/**
	 * Internal TopKFrequentPattern Generation algorithm, which represents the
	 * A's as integers and transforms features to use only integers
	 * 
	 * @param transactions
	 *            Transaction database Iterator
	 * @param attributeFrequency
	 *            array representing the Frequency of the corresponding
	 *            attribute id
	 * @param minSupport
	 *            minimum support of the pattern to be mined
	 * @param k
	 *            Max value of the Size of the Max-Heap in which Patterns are
	 *            held
	 * @param featureSetSize
	 *            number of features
	 * @param returnFeatures
	 *            the id's of the features for which Top K patterns have to be
	 *            mined
	 * @param topKPatternsOutputCollector
	 *            the outputCollector which transforms the given Pattern in
	 *            integer format to the corresponding A Format
	 * @throws MathException
	 */
	private void generateTopKFrequentPatterns(
			// Iterator<Pair<int[], Long>> transactions,
			TransactionTree cTree, OpenObjectIntHashMap<A> attributeIdMapping,
			long[] attributeFrequency, long minSupport, int k,
			int featureSetSize, Collection<Integer> returnFeatures,
			TopKPatternsOutputConverter<A> topKPatternsOutputCollector,
			StatusUpdater updater) throws IOException, MathException {
		// YA: BONSAAAAAAAII {
		// FPTree tree = new FPTree(featureSetSize);
		FPTree tree = null;
		boolean change = true;
		int pruneIters = 0;
		IntArrayList pruneByContingencyCount = new IntArrayList();
		IntArrayList pruneBySpreadCount = new IntArrayList();
		
		while (change) {
			pruneByContingencyCount.add(0);
			pruneBySpreadCount.add(0);
			
			change = false;
			tree = new FPTree(featureSetSize);
			OpenIntLongHashMap[] childJointFreq = new OpenIntLongHashMap[featureSetSize];
			long[] sumChildSupport = new long[featureSetSize];
			double supportGrandTotal = 0;
			// } YA: BONSAAAAAAAII

			for (int i = 0; i < featureSetSize; i++) {
				tree.addHeaderCount(i, attributeFrequency[i]);

				// YA: BONSAAAAAAAII {
				if (attributeFrequency[i] < 0) {
					continue; // this is an attribute not satisfying the
								// monotone constraint
				}
				childJointFreq[i] = new OpenIntLongHashMap();
				supportGrandTotal += attributeFrequency[i];
				// } YA: Bonsai
			}

			// Constructing initial FPTree from the list of transactions
			// YA Bonsai : To pass the tree itself the iterator now would work
			// only with ints.. the A type argument is
			// not checked in the constructor. TOD: remove the type argument and
			// force using ints only
			Iterator<Pair<int[], Long>> transactions = new IntTransactionIterator(
					cTree.iterator(), attributeIdMapping);

			int nodecount = 0;
			// int attribcount = 0;
			int i = 0;
			while (transactions.hasNext()) {
				Pair<int[], Long> transaction = transactions.next();
				Arrays.sort(transaction.getFirst());
				// attribcount += transaction.length;
				// YA: Bonsai {
				// nodecount += treeAddCount(tree, transaction.getFirst(),
				// transaction.getSecond(), minSupport, attributeFrequency);
				int temp = FPTree.ROOTNODEID;
				boolean addCountMode = true;
				for (int attribute : transaction.getFirst()) {
					if (attributeFrequency[attribute] < 0) {
						continue; // this is an attribute not satisfying the
									// monotone constraint
					}
					if (attributeFrequency[attribute] < minSupport) {
						break;
					}
					if (tree.attribute(temp) != -1) { // Root node
						childJointFreq[tree.attribute(temp)].put(
								attribute,
								childJointFreq[tree.attribute(temp)]
										.get(attribute)
										+ transaction.getSecond());
						sumChildSupport[tree.attribute(temp)] += transaction
								.getSecond();
					}
					int child;
					if (addCountMode) {
						child = tree.childWithAttribute(temp, attribute);
						if (child == -1) {
							addCountMode = false;
						} else {
							tree.addCount(child, transaction.getSecond());
							temp = child;
						}
					}
					if (!addCountMode) {
						child = tree.createNode(temp, attribute,
								transaction.getSecond());
						temp = child;
						nodecount++;
					}
				}
				// } YA Bonsai
				i++;
				if (i % 10000 == 0) {
					log.info("FPTree Building: Read {} Transactions", i);
				}
			}

			log.info("Number of Nodes in the FP Tree: {}", nodecount);

			// YA: BONSAAAAAAAII {
			log.info("Bonsai prunining tree: {}", tree.toString());

			for (int a = 0; a < tree.getHeaderTableCount(); ++a) {
				int attr = tree.getAttributeAtIndex(a);

				if (attributeFrequency[attr] < 0) {
					continue; // this is an attribute not satisfying the
								// monotone constraint
				}
				if (attributeFrequency[attr] < minSupport) {
					break;
				}
				if (sumChildSupport[attr] < attributeFrequency[attr]) {
					// the case of . (full stop) as the next child
					childJointFreq[attr]
							.put(-1,
									(long) (attributeFrequency[attr] - sumChildSupport[attr]));
				}
				float numChildren = childJointFreq[attr].size();

				// if (numChildren < LEAST_NUM_CHILDREN_TO_VOTE_FOR_NOISE) {
				// continue;
				// }
				log.trace(
						"Voting for noisiness of attribute {} with number of children: {}",
						attr, numChildren);
				log.trace("Attribute support: {} - Total Children support: {}",
						attributeFrequency[attr], sumChildSupport[attr]);
				// EMD and the such.. the threshold isn't easy to define, and it
				// also doesn't take into account the weights of children.
				// // double uniformProb = 1.0 / numChildren;
				// // double uniformProb = sumChildSupport[attr] /
				// supportGrandTotal;
				// double uniformFreq = attributeFrequency[attr] / numChildren;
				// IntArrayList childAttrArr = childJointFreq[attr].keys();
				// // IntArrayList childAttrArr = new IntArrayList();
				// // childJointFreq[attr].keysSortedByValue(childAttrArr);
				// double totalDifference = 0;
				// double sumOfWeights = 0;
				// // double emd = 0;
				// for (int c = childAttrArr.size() - 1; c >=0 ; --c) {
				// int childAttr = childAttrArr.get(c);
				// double childJF = childJointFreq[attr].get(childAttr);
				// double childWeight = attributeFrequency[childAttr];
				// totalDifference += childWeight * Math.abs(childJF -
				// uniformFreq);
				// sumOfWeights += childWeight;
				//
				// // double jointProb = childJF /
				// // supportGrandTotal;
				// // double childProb = attributeFrequency[childAttr] /
				// // supportGrandTotal;
				// // double childConditional = childJF /
				// attributeFrequency[attr];
				// // emd = childConditional + emd - uniformProb;
				// // emd = childJF + emd - uniformFreq;
				// // totalDifference += Math.abs(emd);
				// }
				// // Probability (D > observed ) = QKS Ne + 0.12 + 0.11/ Ne D
				// // double pNotUniform = totalDifference / attrSupport;
				// // double threshold = (numChildren * (numChildren - 1) * 1.0)
				// // / (2.0 * attributeFrequency[attr]);
				// double weightedDiff = totalDifference / sumOfWeights;
				// double threshold = sumOfWeights / 2.0; // each child can be
				// up to
				// // 1 over or below the
				// // uniform freq
				// boolean noise = weightedDiff < threshold;
				// log.info("EMD: {} - Threshold: {}", weightedDiff, threshold);
				// ///////////////////////////////////
				// Log odds.. this is my hartala, and it needs ot be shifted
				// according to the number of children
				// // // if there is one child then the prob of random choice
				// // will be
				// // // 1, so anything would be
				// // // noise
				// // // and if there are few then the probability that this is
				// // // actually noise declines
				// // if (numChildren >= LEAST_NUM_CHILDREN_TO_VOTE_FOR_NOISE)
				// // {
				// // log.info(
				// //
				// "Voting for noisiness of attribute {} with number of children: {}",
				// // currentAttribute, numChildren);
				// // log.info(
				// // "Attribute support: {} - Total Children support: {}",
				// // attrSupport, sumOfChildSupport);
				// // int noiseVotes = 0;
				// // double randomSelectionLogOdds = 1.0 / numChildren;
				// // randomSelectionLogOdds = Math.log(randomSelectionLogOdds
				// // / (1 - randomSelectionLogOdds));
				// // randomSelectionLogOdds =
				// // Math.abs(randomSelectionLogOdds);
				// //
				// // IntArrayList childAttrArr = childJointFreq.keys();
				// // for (int c = 0; c < childAttrArr.size(); ++c) {
				// // double childConditional = 1.0
				// // * childJointFreq.get(childAttrArr.get(c))
				// // / sumOfChildSupport; // attrSupport;
				// // double childLogOdds = Math.log(childConditional
				// // / (1 - childConditional));
				// // if (Math.abs(childLogOdds) <= randomSelectionLogOdds) {
				// // // probability of the child given me is different
				// // // than
				// // // probability of choosing the
				// // // child randomly
				// // // from among my children.. using absolute log odds
				// // // because they are symmetric
				// // ++noiseVotes;
				// // }
				// // }
				// // log.info("Noisy if below: {} - Noise votes: {}",
				// // randomSelectionLogOdds, noiseVotes);
				// // noise = noiseVotes == numChildren;
				// ////////////////////////////////////////////////////

				// // Kullback-liebler divergence from the uniform distribution
				// double randomChild = 1.0 / numChildren;
				// IntArrayList childAttrArr = childJointFreq[attr].keys();
				//
				// double klDivergence = 0;
				// for (int c = 0; c < childAttrArr.size(); ++c) {
				// double childConditional = 1.0
				// * childJointFreq[attr].get(childAttrArr.get(c))
				// / attributeFrequency[attr];
				// if (childConditional == 0) {
				// continue; // a7a!
				// }
				// klDivergence += childConditional
				// * Math.log(childConditional / randomChild);
				// }
				//
				// boolean noise = Math.abs(klDivergence) < 0.05;
				// log.info("KL-Divergence: {} - Noise less than: {}",
				// klDivergence, 0.05);
				// //////////////////////////////////////
				// Pair wise metric with different children
				SummaryStatistics metricSummary = new SummaryStatistics();
				// double[] metric = new double[(int) numChildren];

				// SummaryStatistics spreadSummary = new SummaryStatistics();
				// double uniformSpread = attributeFrequency[attr] /
				// numChildren;
				double goodnessOfFit = 0.0;
				// If I don't take the . into account: sumChildSupport[attr] /
				// numChildren;

				double sumOfWeights = 0;
				IntArrayList childAttrArr = childJointFreq[attr].keys();
				for (int c = 0; c < childAttrArr.size(); ++c) {
					int childAttr = childAttrArr.get(c);
					double[][] contingencyTable = new double[2][2];
					if (childAttr == -1) {
						// this is meaningless, as yuleq will just be 1
						contingencyTable[1][1] = childJointFreq[attr]
								.get(childAttr);
						contingencyTable[1][0] = sumChildSupport[attr];
						// equals attributeFrequency[attr] -
						// contingencyTable[1][1];
						contingencyTable[0][1] = 0;
						contingencyTable[0][0] = supportGrandTotal
								- attributeFrequency[attr];
					} else {
						contingencyTable[1][1] = childJointFreq[attr]
								.get(childAttr);
						contingencyTable[1][0] = attributeFrequency[attr]
								- contingencyTable[1][1];
						contingencyTable[0][1] = attributeFrequency[childAttr]
								- contingencyTable[1][1];
						contingencyTable[0][0] = supportGrandTotal
								- attributeFrequency[attr]
								- attributeFrequency[childAttr]
								+ contingencyTable[1][1];
						// because of the meninglessness of yuleq in case of . }
						double ad = contingencyTable[0][0]
								* contingencyTable[1][1];
						double bc = contingencyTable[0][1]
								* contingencyTable[1][0];
						double yuleq = (ad - bc) / (ad + bc);
						double weight = attributeFrequency[childAttr];
						sumOfWeights += weight;
						metricSummary.addValue(Math.abs(yuleq * weight));
						// metricSummary.addValue(yuleq * yuleq * weight);
					}
					// spreadSummary.addValue(Math.abs(uniformSpread
					// - contingencyTable[1][1])
					// / numChildren);
					// spreadSummary.addValue(contingencyTable[1][1]); // *
					// weight
					goodnessOfFit += contingencyTable[1][1]
							* contingencyTable[1][1];
				}
				// double weightedquadraticMean =
				// Math.sqrt(metricSummary.getSum() / sumOfWeights);
				double weightedMean = (metricSummary.getSum() / sumOfWeights);

				boolean noise = false;
//				if (weightedMean < 0.5) {
//					pruneByContingencyCount.set(pruneIters, pruneByContingencyCount.get(pruneIters) + 1);
//					noise = true;
//				} else if (weightedMean < 0.95) {
				if(numChildren > 1){
					goodnessOfFit /= (attributeFrequency[attr] / numChildren);
					goodnessOfFit -= attributeFrequency[attr];
					ChiSquaredDistributionImpl chisqDist = new ChiSquaredDistributionImpl(
							numChildren - 1);
					double criticalPoint = chisqDist
							.inverseCumulativeProbability(1.0 - SIGNIFICANCE / 2.0);
					if (goodnessOfFit < criticalPoint) {
						pruneBySpreadCount.set(pruneIters, pruneBySpreadCount.get(pruneIters) + 1);
						noise = true;
					}
					// // double spreadCentraltendency = (spreadSummary.getMax()
					// -
					// // spreadSummary.getMin()) / 2.0;
					// // spreadSummary.getMean();
					// // double uniformSpread = sumChildSupport[attr] /
					// // numChildren;
					//
					// // noise = Math.abs(spreadCentraltendency -
					// uniformSpread) <
					// // 1e-4;
					//
					// double spreadCentraltendency = spreadSummary.getMean();
					// // (spreadSummary.getMax() -
					// // spreadSummary.getMin()) / 2.0;
					// if(spreadCentraltendency < 1e-6){
					// noise = true;
					// }
					//
					// if (!noise && numChildren > 0) {
					// // see if the difference is statitically significant
					// double spreadCI = getConfidenceIntervalHalfWidth(
					// spreadSummary, SIGNIFICANCE);
					// spreadCentraltendency -= spreadCI;
					// if (spreadCentraltendency < 0) {
					// noise = true;
					// }
					// // // noise if the CI contains the uniform spread
					// // threshold
					// // if (spreadCentraltendency > uniformSpread) {
					// // noise = (spreadCentraltendency - spreadCI) <
					// // uniformSpread;
					// // } else {
					// // noise = (spreadCentraltendency + spreadCI) >
					// // uniformSpread;
					// // }
					// }
				}
				change |= noise;

				if (noise) {
					log.info("Pruning attribute {} with child joint freq {}",
							attr, childJointFreq[attr]);
					returnFeatures.remove(attr);
					attributeFrequency[attr] = -1;
					
				}
			}
			++pruneIters;
		}
		log.info("Pruned tree: {}", tree.toString());
		log.info("Prune by contingency: {} - Prune by spread: {}", pruneByContingencyCount.toString(),
				pruneBySpreadCount.toString());
		// } YA: Bonsai
		fpGrowth(tree, minSupport, k, returnFeatures,
				topKPatternsOutputCollector, updater);
	}

	private double getConfidenceIntervalHalfWidth(
			SummaryStatistics summaryStatistics, double significance)
			throws MathException {
		TDistributionImpl tDist = new TDistributionImpl(
				summaryStatistics.getN() - 1);
		double a = tDist.inverseCumulativeProbability(1.0 - significance / 2);
		return a * summaryStatistics.getStandardDeviation()
				/ Math.sqrt(summaryStatistics.getN());
	}

	private static FrequentPatternMaxHeap growth(FPTree tree,
			MutableLong minSupportMutable, int k, FPTreeDepthCache treeCache,
			int level, int currentAttribute, StatusUpdater updater) {

		int i = Arrays.binarySearch(tree.getHeaderTableAttributes(),
				currentAttribute);
		if (i < 0) {
			return new FrequentPatternMaxHeap(k, true); // frequentPatterns;
		}

		// YA: The frequent pattern returned should be k PER item.. so the total
		// size of the heap should be k * number of items (headerTableCount - i)
		int headerTableCount = tree.getHeaderTableCount();
		FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(k
				* (headerTableCount - i), true);

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
						minSupportMutable.longValue(), conditionalTree, tree);
				// printTree(conditionalTree);

			}

			FrequentPatternMaxHeap returnedPatterns;
			if (attribute == currentAttribute) {

				returnedPatterns = growthTopDown(conditionalTree,
						minSupportMutable, k, treeCache, level + 1, true,
						currentAttribute, updater);

				frequentPatterns = mergeHeap(frequentPatterns,
						returnedPatterns, attribute, count, true);
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
			MutableLong minSupportMutable, int k, FPTreeDepthCache treeCache,
			int level, boolean conditionalOfCurrentAttribute,
			int currentAttribute, StatusUpdater updater) {

		FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(k,
				false);

		if (!conditionalOfCurrentAttribute) {
			int index = Arrays.binarySearch(tree.getHeaderTableAttributes(),
					currentAttribute);
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
						minSupportMutable.longValue(), conditionalTree, tree);
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
							minSupportMutable, k, treeCache, level + 1, true,
							currentAttribute, updater);

					frequentPatterns = mergeHeap(frequentPatterns,
							returnedPatterns, attribute, count, true);
				} else if (attribute > currentAttribute) {
					traverseAndBuildConditionalFPTreeData(
							tree.getHeaderNext(attribute),
							minSupportMutable.longValue(), conditionalTree,
							tree);
					returnedPatterns = growthBottomUp(conditionalTree,
							minSupportMutable, k, treeCache, level + 1, false,
							currentAttribute, updater);
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
			MutableLong minSupportMutable, int k, FPTreeDepthCache treeCache,
			int level, boolean conditionalOfCurrentAttribute,
			int currentAttribute, StatusUpdater updater) {

		FrequentPatternMaxHeap frequentPatterns = new FrequentPatternMaxHeap(k,
				true);

		if (!conditionalOfCurrentAttribute) {
			int index = Arrays.binarySearch(tree.getHeaderTableAttributes(),
					currentAttribute);
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
						minSupportMutable.longValue(), conditionalTree, tree);

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
							minSupportMutable, k, treeCache, level + 1, true,
							currentAttribute, updater);
					frequentPatterns = mergeHeap(frequentPatterns,
							returnedPatterns, attribute, count, true);

				} else if (attribute > currentAttribute) {
					traverseAndBuildConditionalFPTreeData(
							tree.getHeaderNext(attribute),
							minSupportMutable.longValue(), conditionalTree,
							tree);
					returnedPatterns = growthBottomUp(conditionalTree,
							minSupportMutable, k, treeCache, level + 1, false,
							currentAttribute, updater);
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

	private static FrequentPatternMaxHeap mergeHeap(
			FrequentPatternMaxHeap frequentPatterns,
			FrequentPatternMaxHeap returnedPatterns, int attribute, long count,
			boolean addAttribute) {
		frequentPatterns.addAll(returnedPatterns, attribute, count);
		if (frequentPatterns.addable(count) && addAttribute) {
			Pattern p = new Pattern();
			p.add(attribute, count);
			frequentPatterns.insert(p);
		}

		return frequentPatterns;
	}

	private static void traverseAndBuildConditionalFPTreeData(
			int firstConditionalNode, long minSupport, FPTree conditionalTree,
			FPTree tree) {

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
					tree.setConditional(pathNode,
							conditionalTree.createConditionalNode(attribute, 0));
					conditional = tree.conditional(pathNode);
					conditionalTree.addHeaderNext(attribute, conditional);
				} else {
					conditionalTree.setSinglePath(false);
				}

				if (prevConditional != -1) { // if there is a child element
					int prevParent = conditionalTree.parent(prevConditional);
					if (prevParent == -1) {
						conditionalTree.setParent(prevConditional, conditional);
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

	// The FPTree strcture doesn't really support pruning..
	// private static void pruneFPTree(long minSupport, FPTree tree) {
	// log.info("Prunining conditional Tree: {}", tree.toString());
	// for (int i = 0; i < tree.getHeaderTableCount(); i++) {
	// // for (int i = tree.getHeaderTableCount() - 1; i >= 0; --i) {
	// int currentAttribute = tree.getAttributeAtIndex(i);
	// float attrSupport = tree.getHeaderSupportCount(currentAttribute);
	// boolean rare = attrSupport < minSupport;
	// boolean noise = false;
	//
	// if (!rare) {
	//
	// OpenIntLongHashMap childJointFreq = new OpenIntLongHashMap();
	// float sumOfChildSupport = 0;
	// int nextNode = tree.getHeaderNext(currentAttribute);
	// while (nextNode != -1) {
	// int mychildCount = tree.childCount(nextNode);
	//
	// for (int j = 0; j < mychildCount; j++) {
	// int myChildId = tree.childAtIndex(nextNode, j);
	// int myChildAttr = tree.attribute(myChildId);
	// long childSupport = tree.count(myChildId);
	// sumOfChildSupport += childSupport;
	// childJointFreq.put(myChildAttr,
	// childJointFreq.get(myChildAttr) + childSupport);
	// }
	// nextNode = tree.next(nextNode);
	// }
	//
	// if (sumOfChildSupport < attrSupport) {
	// childJointFreq.put(-1,
	// (long) (attrSupport - sumOfChildSupport));
	// }
	// float numChildren = childJointFreq.size();
	//
	// if (numChildren >= LEAST_NUM_CHILDREN_TO_VOTE_FOR_NOISE) {
	// log.info(
	// "Voting for noisiness of attribute {} with number of children: {}",
	// currentAttribute, numChildren);
	// log.info(
	// "Attribute support: {} - Total Children support: {}",
	// attrSupport, sumOfChildSupport);
	// double uniformProb = 1.0 / numChildren;
	//
	// IntArrayList childAttrArr = childJointFreq.keys();
	// double totalDifference = 0;
	// double emd = 0;
	// for (int c = 0; c < childAttrArr.size(); ++c) {
	// double childConditional = 1.0
	// * childJointFreq.get(childAttrArr.get(c))
	// / attrSupport;
	// emd = childConditional + emd - uniformProb;
	// totalDifference += Math.abs(emd);
	// }
	// // Probability (D > observed ) = QKS Ne + 0.12 + 0.11/ Ne D
	// // double pNotUniform = totalDifference / attrSupport;
	// double threshold = (attrSupport / numChildren);
	// noise = totalDifference < threshold;
	// log.info("EMD: {} - Threshold: {}", totalDifference,
	// threshold);
	//
	// // // if there is one child then the prob of random choice
	// // will be
	// // // 1, so anything would be
	// // // noise
	// // // and if there are few then the probability that this is
	// // // actually noise declines
	// // if (numChildren >= LEAST_NUM_CHILDREN_TO_VOTE_FOR_NOISE)
	// // {
	// // log.info(
	// // "Voting for noisiness of attribute {} with number of children: {}",
	// // currentAttribute, numChildren);
	// // log.info(
	// // "Attribute support: {} - Total Children support: {}",
	// // attrSupport, sumOfChildSupport);
	// // int noiseVotes = 0;
	// // double randomSelectionLogOdds = 1.0 / numChildren;
	// // randomSelectionLogOdds = Math.log(randomSelectionLogOdds
	// // / (1 - randomSelectionLogOdds));
	// // randomSelectionLogOdds =
	// // Math.abs(randomSelectionLogOdds);
	// //
	// // IntArrayList childAttrArr = childJointFreq.keys();
	// // for (int c = 0; c < childAttrArr.size(); ++c) {
	// // double childConditional = 1.0
	// // * childJointFreq.get(childAttrArr.get(c))
	// // / sumOfChildSupport; // attrSupport;
	// // double childLogOdds = Math.log(childConditional
	// // / (1 - childConditional));
	// // if (Math.abs(childLogOdds) <= randomSelectionLogOdds) {
	// // // probability of the child given me is different
	// // // than
	// // // probability of choosing the
	// // // child randomly
	// // // from among my children.. using absolute log odds
	// // // because they are symmetric
	// // ++noiseVotes;
	// // }
	// // }
	// // log.info("Noisy if below: {} - Noise votes: {}",
	// // randomSelectionLogOdds, noiseVotes);
	// // noise = noiseVotes == numChildren;
	//
	// // double randomChild = 1.0 / numChildren;
	// // IntArrayList childAttrArr = childJointFreq.keys();
	// //
	// // float klDivergence = 0;
	// // for (int c = 0; c < childAttrArr.size(); ++c) {
	// // double childConditional = 1.0
	// // * childJointFreq.get(childAttrArr.get(c))
	// // / attrSupport; // sumOfChildSupport;
	// // if (childConditional == 0) {
	// // continue; // a7a!
	// // }
	// // klDivergence += childConditional
	// // * Math.log(childConditional / randomChild);
	// // }
	// //
	// // noise = Math.abs(klDivergence) < 0.05;
	// // log.info("KL-Divergence: {} - Noise less than: {}",
	// // klDivergence, 0.05);
	//
	// }
	//
	// }
	//
	// if (noise || rare) {
	// int nextNode = tree.getHeaderNext(currentAttribute);
	// tree.removeHeaderNext(currentAttribute);
	// while (nextNode != -1) {
	//
	// int mychildCount = tree.childCount(nextNode);
	// int parentNode = tree.parent(nextNode);
	//
	// if (mychildCount > 0) {
	// tree.replaceChild(parentNode, nextNode,
	// tree.childAtIndex(nextNode, 0));
	// for (int j = 1; j < mychildCount; j++) {
	// Integer myChildId = tree.childAtIndex(nextNode, j);
	// // YA: This will work for the first child only
	// // tree.replaceChild(parentNode, nextNode,
	// // myChildId);
	// tree.addChild(parentNode, myChildId);
	// tree.setParent(myChildId, parentNode);
	// }
	// } else {
	// // There is no support for deleting children.. so leaf
	// // nodes will stay!!
	// // tree.replaceChild(parentNode, nextNode, childnodeId)
	// }
	// nextNode = tree.next(nextNode);
	// }
	//
	// }
	// }
	//
	// // for (int i = 0; i < tree.getHeaderTableCount(); i++) {
	// // int currentAttribute = tree.getAttributeAtIndex(i);
	// // int nextNode = tree.getHeaderNext(currentAttribute);
	// //
	// // OpenIntIntHashMap prevNode = new OpenIntIntHashMap();
	// // int justPrevNode = -1;
	// // while (nextNode != -1) {
	// //
	// // int parent = tree.parent(nextNode);
	// //
	// // if (prevNode.containsKey(parent)) {
	// // int prevNodeId = prevNode.get(parent);
	// // if (tree.childCount(prevNodeId) <= 1
	// // && tree.childCount(nextNode) <= 1) {
	// // tree.addCount(prevNodeId, tree.count(nextNode));
	// // tree.addCount(nextNode, -1 * tree.count(nextNode));
	// // if (tree.childCount(nextNode) == 1) {
	// // tree.addChild(prevNodeId,
	// // tree.childAtIndex(nextNode, 0));
	// // tree.setParent(tree.childAtIndex(nextNode, 0),
	// // prevNodeId);
	// // }
	// // tree.setNext(justPrevNode, tree.next(nextNode));
	// // }
	// // } else {
	// // prevNode.put(parent, nextNode);
	// // }
	// // justPrevNode = nextNode;
	// // nextNode = tree.next(nextNode);
	// // }
	// // }
	//
	// // prune Conditional Tree
	//
	// }

	private static void pruneFPTree(long minSupport, FPTree tree) {
		if (log.isTraceEnabled())
			log.trace("Prunining conditional Tree: {}", tree.toString());
		for (int i = 0; i < tree.getHeaderTableCount(); i++) {
			int currentAttribute = tree.getAttributeAtIndex(i);
			float attrSupport = tree.getHeaderSupportCount(currentAttribute);
			if (attrSupport < minSupport) {
				int nextNode = tree.getHeaderNext(currentAttribute);
				tree.removeHeaderNext(currentAttribute);
				while (nextNode != -1) {

					int mychildCount = tree.childCount(nextNode);
					int parentNode = tree.parent(nextNode);

					if (mychildCount > 0) {
						tree.replaceChild(parentNode, nextNode,
								tree.childAtIndex(nextNode, 0));
						for (int j = 1; j < mychildCount; j++) {
							Integer myChildId = tree.childAtIndex(nextNode, j);
							// YA: This will work for the first child only
							// tree.replaceChild(parentNode, nextNode,
							// myChildId);
							tree.addChild(parentNode, myChildId);
							tree.setParent(myChildId, parentNode);
						}
					} else {
						// There is no support for deleting children.. so leaf
						// nodes will stay!!
						// tree.replaceChild(parentNode, nextNode, childnodeId)
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

	// /**
	// * Create FPTree with node counts incremented by addCount variable given
	// the
	// * root node and the List of Attributes in transaction sorted by support
	// *
	// * @param tree
	// * object to which the transaction has to be added to
	// * @param myList
	// * List of transactions sorted by support
	// * @param addCount
	// * amount by which the Node count has to be incremented
	// * @param minSupport
	// * the MutableLong value which contains the current
	// * value(dynamic) of support
	// * @param attributeFrequency
	// * the list of attributes and their frequency
	// * @return the number of new nodes added
	// */
	// private static int treeAddCount(FPTree tree, int[] myList, long addCount,
	// long minSupport, long[] attributeFrequency) {
	//
	// int temp = FPTree.ROOTNODEID;
	// int ret = 0;
	// boolean addCountMode = true;
	//
	// for (int attribute : myList) {
	// if (attributeFrequency[attribute] < minSupport) {
	// return ret;
	// }
	// int child;
	// if (addCountMode) {
	// child = tree.childWithAttribute(temp, attribute);
	// if (child == -1) {
	// addCountMode = false;
	// } else {
	// tree.addCount(child, addCount);
	// temp = child;
	// }
	// }
	// if (!addCountMode) {
	// child = tree.createNode(temp, attribute, addCount);
	// temp = child;
	// ret++;
	// }
	// }
	//
	// return ret;
	//
	// }
}
