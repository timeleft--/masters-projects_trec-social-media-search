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

package org.apache.mahout.freqtermsets;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.convertors.ContextStatusUpdater;
import org.apache.mahout.freqtermsets.convertors.ContextWriteOutputCollector;
import org.apache.mahout.freqtermsets.convertors.integer.IntegerStringOutputConverter;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.freqtermsets.fpgrowth.FPStream;
import org.apache.mahout.freqtermsets.stream.TimeWeightFunction;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectLongHashMap;

import ca.uwaterloo.twitter.TokenIterator;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

/**
 * takes each group of transactions and runs Vanilla FPGrowth on it and
 * outputs the the Top K frequent Patterns for each group.
 * 
 */
public class ParallelFPStreamReducer extends
    Reducer<IntWritable, TransactionTree, Text, TopKStringPatterns> {
  
  public static final String MIN_WORDS_FOR_LANG_ID = "lenLangId";
  public static final int MIN_WORDS_FOR_LANG_ID_DEFAULT = 3;
  
  // private final List<String> featureReverseMap = Lists.newArrayList();
  // private final LongArrayList freqList = new LongArrayList();
  // private final LinkedHashMap<Integer, String> reverseMap = Maps.newLinkedHashMap();
  private final OpenIntObjectHashMap<String> idStringMap = new OpenIntObjectHashMap<String>();
  // private final OpenObjectIntHashMap<Integer> ixIdMap = new OpenObjectIntHashMap<Integer>();
  // private final OpenIntObjectHashMap<Integer> idIxMap = new OpenIntObjectHashMap<Integer>();
  // private final OpenIntIntHashMap idFreqMap = new OpenIntIntHashMap();
  
  private int maxHeapSize = 50;
  
  private int minSupport = 3;
  
  private int numFeatures;
  // private int maxPerGroup;
  private int numGroups;
  
  // private boolean useFP2 = true;
  private int minWordsForLangDetection;
  // private double superiorityRatio;
  private boolean repeatHashTag;
  private long intervalStart;
  private long intervalEnd;
  private long windowSize;
  private long endTimestamp;
  
  private static class IteratorAdapter implements Iterator<Pair<List<Integer>, Long>> {
    private Iterator<Pair<IntArrayList, Long>> innerIter;
    
    private IteratorAdapter(Iterator<Pair<IntArrayList, Long>> transactionIter) {
      innerIter = transactionIter;
    }
    
    @Override
    public boolean hasNext() {
      return innerIter.hasNext();
    }
    
    @Override
    public Pair<List<Integer>, Long> next() {
      Pair<IntArrayList, Long> innerNext = innerIter.next();
      return new Pair(innerNext.getFirst().toList(), innerNext.getSecond());
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  @Override
  protected void reduce(IntWritable key, Iterable<TransactionTree> values, Context context)
      throws IOException {
    TransactionTree cTree = new TransactionTree();
    for (TransactionTree tr : values) {
      for (Pair<IntArrayList, Long> p : tr) {
        cTree.addPattern(p.getFirst(), p.getSecond());
      }
    }
    
    List<Pair<Integer, Long>> localFList = Lists.newArrayList();
    for (Entry<Integer, MutableLong> fItem : cTree.generateFList().entrySet()) {
      localFList.add(new Pair<Integer, Long>(fItem.getKey(), fItem.getValue().toLong()));
    }
    
    Collections.sort(localFList, new CountDescendingPairComparator<Integer, Long>());
    
    // if (useFP2) {
    // org.apache.mahout.freqtermsets.fpgrowth2.FPGrowthIds fpGrowth =
    // new org.apache.mahout.freqtermsets.fpgrowth2.FPGrowthIds(featureReverseMap);
    // fpGrowth.generateTopKFrequentPatterns(
    // cTree.iterator(),
    // freqList,
    // minSupport,
    // maxHeapSize,
    // PFPGrowth.getGroupMembers(key.get(), maxPerGroup, numFeatures),
    // new IntegerStringOutputConverter(
    // new
    // ContextWriteOutputCollector<IntWritable,TransactionTree,Text,TopKStringPatterns>(context),
    // featureReverseMap,minWordsForLangDetection/*, superiorityRatio*/),
    // new ContextStatusUpdater<IntWritable,TransactionTree,Text,TopKStringPatterns>(context));
    // } else {
    
    FPStream<Integer> fpStream = new FPStream<Integer>();
    fpStream
        .generateTopKFrequentPatterns(
            new IteratorAdapter(cTree.iterator()),
            localFList,
            minSupport,
            maxHeapSize,
            // new HashSet<Integer>(PFPGrowth.getGroupMembers(key.get(),
            // maxPerGroup,
            // numFeatures).toList()),
            new IntegerStringOutputConverter(
                new ContextWriteOutputCollector<IntWritable, TransactionTree, Text, TopKStringPatterns>(
                    context),
                // featureReverseMap,
                idStringMap,
                minWordsForLangDetection/* , superiorityRatio */, repeatHashTag),
            new ContextStatusUpdater<IntWritable, TransactionTree, Text, TopKStringPatterns>(
                context),
            // ixIdMap,
            // idIxMap,
            // idFreqMap,
            // idStringMap,
            key.get(),
            numGroups);
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
    
    intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP))); //GMT23JAN2011)));
    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE)));
    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize - 1);
    
    OpenObjectLongHashMap<String> prevFLists = PFPGrowth.readOlderCachedFLists(context
        .getConfiguration(),
        intervalStart, TimeWeightFunction.getDefault(params));
    
    LinkedList<String> terms = Lists.newLinkedList();
    prevFLists.keysSortedByValue(terms);
    Iterator<String> termsIter = terms.descendingIterator();
    int ix = 0;
    while (termsIter.hasNext()) {
      
      String t = termsIter.next();
      // featureReverseMap.add(e.getFirst());
      // freqList.add(e.getSecond());
      int id = Hashing.murmur3_32().hashString(t, Charset.forName("UTF-8")).asInt();
      int c = 0;
      while (idStringMap.containsKey(id)) {
        // Best effort
        if (c < t.length()) {
          id = Hashing.murmur3_32((int) t.charAt(c++)).hashString(t, Charset.forName("UTF-8"))
              .asInt();
        } else {
          ++id;
        }
      }
      
      // idFreqMap.put(id, e.getSecond().intValue());
      idStringMap.put(id, t);
      
      // idIxMap.put(id, ix);
      // ixIdMap.put(ix, id);
      
      ++ix;
    }
    
    maxHeapSize = Integer.valueOf(params.get(PFPGrowth.MAX_HEAPSIZE, "50"));
    minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
    
    // maxPerGroup = params.getInt(PFPGrowth.MAX_PER_GROUP, 0);
    numGroups = params.getInt(PFPGrowth.NUM_GROUPS, PFPGrowth.NUM_GROUPS_DEFAULT);
    
    // numFeatures = featureReverseMap.size();
    numFeatures = ix;
    
    // useFP2 = "true".equals(params.get(PFPGrowth.USE_FPG2));
    minWordsForLangDetection = params.getInt(MIN_WORDS_FOR_LANG_ID, MIN_WORDS_FOR_LANG_ID_DEFAULT);
    // superiorityRatio = Double.parseDouble(params.get(SUPERIORITY_RATIO_PARAM, "1.11"));
    // useFP2 = !"false".equals(params.get(PFPGrowth.USE_FPG2));
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
  }
}
