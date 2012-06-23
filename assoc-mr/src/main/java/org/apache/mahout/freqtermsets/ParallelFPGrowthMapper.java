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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;

import ca.uwaterloo.twitter.TokenIterator;
import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;
import edu.umd.cloud9.io.pair.PairOfStringLong;

/**
 * maps each transaction to all unique items groups in the transaction. mapper
 * outputs the group id as key and the transaction as value
 * 
 */
public class ParallelFPGrowthMapper extends
    Mapper<PairOfStringLong, Text, IntWritable, TransactionTree> {
  
  private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
  // private Pattern splitter;
  private int maxPerGroup;
  private long intervalStart;
  private long intervalEnd;
  
  private IntWritable wGroupID = new IntWritable();
  private boolean repeatHashTag;
  private long windowSize;
  private long endTimestamp;
  private boolean prependUserName;
  
  @Override
  protected void map(PairOfStringLong key, Text input, Context context)
      throws IOException, InterruptedException {
    
    String screenname = key.getLeftElement();
    long timestamp = key.getRightElement();
    if (timestamp < intervalStart) {
      return;
    } else if (timestamp > endTimestamp) {
      // I won't assume that the sequential order is reliable
      // is it possible for a mapper to say its the end anyway?
      return;
    }
    
    // String[] items = splitter.split(input.toString());
    
    OpenIntHashSet itemSet = new OpenIntHashSet();
    
    String inputStr;
    // for (String item : items) {
    if(prependUserName){
      inputStr = "@" + screenname + ": " + input;
    } else {
      inputStr = input.toString();
    }
    
    LatinTokenIterator items = new LatinTokenIterator(inputStr);
    items.setRepeatHashTag(repeatHashTag);
    while (items.hasNext()) {
      String item = items.next();
      if (fMap.containsKey(item) && !item.trim().isEmpty()) {
        itemSet.add(fMap.get(item));
      }
    }
    
    IntArrayList itemArr = new IntArrayList(itemSet.size());
    itemSet.keys(itemArr);
    // YA: why is sort needed here? won't group dependent transactions (below) become
    // just monotonically increasing lists of items because of this?
    itemArr.sort();
    
    OpenIntHashSet groups = new OpenIntHashSet();
    for (int j = itemArr.size() - 1; j >= 0; j--) {
      // generate group dependent shards
      int item = itemArr.get(j);
      int groupID = PFPGrowth.getGroup(item, maxPerGroup);
      
      if (!groups.contains(groupID)) {
        IntArrayList tempItems = new IntArrayList(j + 1);
        tempItems.addAllOfFromTo(itemArr, 0, j);
        context
            .setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: " + item);
        wGroupID.set(groupID);
        context.write(wGroupID, new TransactionTree(tempItems, 1L));
      }
      groups.add(groupID);
    }
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    
    int i = 0;
    for (Pair<String, Long> e : PFPGrowth.readFList(context.getConfiguration())) {
      fMap.put(e.getFirst(), i++);
    }
    
    Parameters params =
        new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
    
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
    
    // splitter = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN,
    // PFPGrowth.SPLITTER.toString()));
    
    maxPerGroup = Integer.valueOf(params.getInt(PFPGrowth.MAX_PER_GROUP, 0));
    
    intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
//        Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP))); //GMT23JAN2011)));
    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
//        Long.toString(Long.MAX_VALUE)));
    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize - 1);
    
    prependUserName = true;
  }
}
