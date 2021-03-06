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
import org.apache.mahout.common.Parameters;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;

import ca.uwaterloo.twitter.TokenIterator;
import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;
import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStringLong;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * maps each transaction to all unique items groups in the transaction. mapper
 * outputs the group id as key and the transaction as value
 * 
 */
public class ParallelFPStreamMapper extends
    Mapper<PairOfLongs, PairOfStrings, IntWritable, TransactionTree> {
  private final OpenObjectIntHashMap<String> stringToIdMap = new OpenObjectIntHashMap<String>();
  private int numGroups;
  private long intervalStart;
  private long intervalEnd;
  
  private IntWritable wGroupID = new IntWritable();
  private boolean repeatHashTag;
  private long windowSize;
  private long endTimestamp;
  private boolean prependUserName;
  
  @Override
  protected void map(PairOfLongs key, PairOfStrings input, Context context)
      throws IOException, InterruptedException {
    
    long timestamp = key.getRightElement();
    if (timestamp < intervalStart) {
      return;
    } else if (timestamp > endTimestamp) {
      // I won't assume that the sequential order is reliable
      // is it possible for a mapper to say its the end anyway?
      return;
    }
    
    OpenIntObjectHashMap<String> itemSet = new OpenIntObjectHashMap<String>();
    
    String inputStr;
    if (prependUserName) {
      inputStr = "@" + input.getLeftElement() + ": " + input.getRightElement();
    } else {
      inputStr = input.getRightElement();
    }
    
    LatinTokenIterator items = new LatinTokenIterator(inputStr);
    items.setRepeatHashTag(repeatHashTag);
    while (items.hasNext()) {
      String item = items.next();
      if (stringToIdMap.containsKey(item) && !item.trim().isEmpty()) {
        itemSet.put(stringToIdMap.get(item), item);
      }
    }
    
    IntArrayList itemArr = new IntArrayList(itemSet.size());
    itemSet.keys(itemArr);
    itemArr.sort();
    
    OpenIntHashSet groups = new OpenIntHashSet();
    for (int j = itemArr.size() - 1; j >= 0; j--) {
      // generate group dependent shards
      int itemId = itemArr.get(j);
      // int groupID = PFPGrowth.getGroup(item, maxPerGroup);
      
      int groupID = PFPGrowth.getGroupFromHash(itemId, numGroups);
      
      if (!groups.contains(groupID)) {
        String item = itemSet.get(itemId);
        
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
    Parameters params =
        new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
    
    intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize - 1);
    
    PFPGrowth.loadEarlierFHashMaps(context,
        params,
        intervalStart,
        new OpenIntObjectHashMap<String>(),
        stringToIdMap);
    
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
    
    numGroups = params.getInt(PFPGrowth.NUM_GROUPS, PFPGrowth.NUM_GROUPS_DEFAULT);
    
    prependUserName = true;
  }
}
