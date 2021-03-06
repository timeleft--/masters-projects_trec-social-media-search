/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.freqtermsets;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.mahout.freqtermsets.fpgrowth.FPGrowth;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.list.LongArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;

import ca.uwaterloo.twitter.TokenIterator;

import com.google.common.collect.Lists;

/**
 *  takes each group of transactions and runs Vanilla FPGrowth on it and
 * outputs the the Top K frequent Patterns for each group.
 * 
 */
public class ParallelFPGrowthReducer extends Reducer<IntWritable,TransactionTree,Text,TopKStringPatterns> {

  public static final String MIN_WORDS_FOR_LANG_ID = "lenLangId";
  public static final int MIN_WORDS_FOR_LANG_ID_DEFAULT = 3;
  
  private final OpenIntObjectHashMap<String> featureReverseMap = new OpenIntObjectHashMap<String>();
  private final LongArrayList freqList = new LongArrayList();
  
  private int maxHeapSize = 50;
  
  private int minSupport = 3;

  private int numFeatures;
  private int maxPerGroup;

//  private boolean useFP2 = true;
  private int minWordsForLangDetection;
//  private double superiorityRatio;
  private boolean repeatHashTag;

  public static class IteratorAdapter implements Iterator<Pair<List<Integer>,Long>> {
    private Iterator<Pair<IntArrayList,Long>> innerIter;

    public IteratorAdapter(Iterator<Pair<IntArrayList,Long>> transactionIter) {
      innerIter = transactionIter;
    }

    @Override
    public boolean hasNext() {
      return innerIter.hasNext();
    }

    @Override
    public Pair<List<Integer>,Long> next() {
      Pair<IntArrayList,Long> innerNext = innerIter.next();
      return new Pair(innerNext.getFirst().toList(), innerNext.getSecond());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected void reduce(IntWritable key, Iterable<TransactionTree> values, Context context) throws IOException {
    TransactionTree cTree = new TransactionTree();
    for (TransactionTree tr : values) {
      for (Pair<IntArrayList,Long> p : tr) {
        cTree.addPattern(p.getFirst(), p.getSecond());
      }
    }
    
    List<Pair<Integer,Long>> localFList = Lists.newArrayList();
    for (Entry<Integer,MutableLong> fItem : cTree.generateFList().entrySet()) {
      localFList.add(new Pair<Integer,Long>(fItem.getKey(), fItem.getValue().toLong()));
    }
    
    Collections.sort(localFList, new CountDescendingPairComparator<Integer,Long>());
    
//    if (useFP2) {
//      org.apache.mahout.freqtermsets.fpgrowth2.FPGrowthIds fpGrowth = 
//        new org.apache.mahout.freqtermsets.fpgrowth2.FPGrowthIds(featureReverseMap);
//      fpGrowth.generateTopKFrequentPatterns(
//          cTree.iterator(),
//          freqList,
//          minSupport,
//          maxHeapSize,
//          PFPGrowth.getGroupMembers(key.get(), maxPerGroup, numFeatures),
//          new IntegerStringOutputConverter(
//              new ContextWriteOutputCollector<IntWritable,TransactionTree,Text,TopKStringPatterns>(context),
//              featureReverseMap,minWordsForLangDetection/*, superiorityRatio*/),
//          new ContextStatusUpdater<IntWritable,TransactionTree,Text,TopKStringPatterns>(context));
//    } else {
      FPGrowth<Integer> fpGrowth = new FPGrowth<Integer>();
      fpGrowth.generateTopKFrequentPatterns(
//          new IteratorAdapter(cTree.iterator()),
          cTree,
          localFList,
          minSupport,
          maxHeapSize,
          new HashSet<Integer>(PFPGrowth.getGroupMembers(key.get(), 
                                                         maxPerGroup, 
                                                         numFeatures).toList()),
          new IntegerStringOutputConverter(
              new ContextWriteOutputCollector<IntWritable,TransactionTree,Text,TopKStringPatterns>(context),
              featureReverseMap,minWordsForLangDetection/*, superiorityRatio*/, repeatHashTag),
          new ContextStatusUpdater<IntWritable,TransactionTree,Text,TopKStringPatterns>(context),
          -1,-1); //those will not be used as long as there is something in the returnable features
//    }
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
    
    int ix = 0;
    for (Pair<String,Long> e : PFPGrowth.readCachedFList(context.getConfiguration())) {
      featureReverseMap.put(ix++,e.getFirst());
      freqList.add(e.getSecond());
    }
    
    maxHeapSize = Integer.valueOf(params.get(PFPGrowth.MAX_HEAPSIZE, "50"));
    minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));

    maxPerGroup = params.getInt(PFPGrowth.MAX_PER_GROUP, 0);
    numFeatures = featureReverseMap.size();
//    useFP2 = "true".equals(params.get(PFPGrowth.USE_FPG2));
    minWordsForLangDetection = params.getInt(MIN_WORDS_FOR_LANG_ID, MIN_WORDS_FOR_LANG_ID_DEFAULT);
//    superiorityRatio = Double.parseDouble(params.get(SUPERIORITY_RATIO_PARAM, "1.11"));
//    useFP2 = !"false".equals(params.get(PFPGrowth.USE_FPG2));
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
  }
}
