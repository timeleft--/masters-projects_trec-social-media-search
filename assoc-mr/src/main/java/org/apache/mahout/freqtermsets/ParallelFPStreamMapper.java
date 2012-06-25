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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.stream.TimeWeightFunction;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.map.OpenObjectLongHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;

import ca.uwaterloo.twitter.ItemSetIndexBuilder;
import ca.uwaterloo.twitter.TokenIterator;
import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import edu.umd.cloud9.io.pair.PairOfStringLong;

/**
 * maps each transaction to all unique items groups in the transaction. mapper
 * outputs the group id as key and the transaction as value
 * 
 */
public class ParallelFPStreamMapper extends
    Mapper<PairOfStringLong, Text, IntWritable, TransactionTree> {
  private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
  private int numGroups;
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
    
    OpenIntObjectHashMap<String> itemSet = new OpenIntObjectHashMap<String>();
    
    String inputStr;
    if (prependUserName) {
      inputStr = "@" + screenname + ": " + input;
    } else {
      inputStr = input.toString();
    }
    
    LatinTokenIterator items = new LatinTokenIterator(inputStr);
    items.setRepeatHashTag(repeatHashTag);
    while (items.hasNext()) {
      String item = items.next();
      if (fMap.containsKey(item) && !item.trim().isEmpty()) {
        itemSet.put(fMap.get(item), item);
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
     
      int groupID = PFPGrowth.getGroupHash(itemId, numGroups);
      
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
    
    OpenIntHashSet usedIds = new OpenIntHashSet();
    OpenObjectLongHashMap<String> prevFLists = PFPGrowth.readOlderCachedFLists(context
        .getConfiguration(),
        intervalStart, TimeWeightFunction.getDefault(params));
    
    LinkedList<String> terms = Lists.newLinkedList();
    prevFLists.keysSortedByValue(terms);
    Iterator<String> termsIter = terms.descendingIterator();
    while (termsIter.hasNext()) {
      String t = termsIter.next();
      int id = Hashing.murmur3_32().hashString(t, Charset.forName("UTF-8")).asInt();
      int c = 0;
      while (usedIds.contains(id)) {
        // Best effort
        if (c < t.length()) {
          id = Hashing.murmur3_32((int) t.charAt(c++)).hashString(t, Charset.forName("UTF-8"))
              .asInt();
        } else {
          ++id;
        }
      }
      fMap.put(t, id);
      usedIds.add(id);
    }
    
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
    
    numGroups = params.getInt(PFPGrowth.NUM_GROUPS, PFPGrowth.NUM_GROUPS_DEFAULT);
    
    prependUserName = true;
  }
}
