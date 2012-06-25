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
  
  private static final boolean PATTERNS_FROM_EARLIER_WINDOWS = false;

  private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
  // private final OpenIntObjectHashMap<String> idToString = new OpenIntObjectHashMap<String>();
  
  // private Pattern splitter;
  // private int maxPerGroup;
  private int numGroups;
  private long intervalStart;
  private long intervalEnd;
  
  private IntWritable wGroupID = new IntWritable();
  private boolean repeatHashTag;
  private long windowSize;
  private long endTimestamp;
  private boolean prependUserName;
  
  private IndexReader fisIxReader;
  private TimeWeightFunction timeWeigth;
  private long mostRecentTime;
  
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
    
    // OpenIntHashSet itemSet = new OpenIntHashSet();
    OpenIntObjectHashMap<String> itemSet = new OpenIntObjectHashMap<String>();
    
    String inputStr;
    // for (String item : items) {
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
    // YA: why is sort needed here? won't group dependent transactions (below) become
    // just monotonically increasing lists of items because of this?
    itemArr.sort();
    
    OpenIntHashSet groups = new OpenIntHashSet();
    for (int j = itemArr.size() - 1; j >= 0; j--) {
      // generate group dependent shards
      int itemId = itemArr.get(j);
      // int groupID = PFPGrowth.getGroup(item, maxPerGroup);
      
      int groupID = PFPGrowth.getGroupHash(itemId, numGroups);
      
      if (!groups.contains(groupID)) {
        // String item = idToString.get(itemId);
        String item = itemSet.get(itemId);
        
        IntArrayList tempItems = new IntArrayList(j + 1);
        tempItems.addAllOfFromTo(itemArr, 0, j);
        context
            .setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: " + item);
        wGroupID.set(groupID);
        
//        TransactionTree patternTree = new TransactionTree();
//        patternTree.addPattern(tempItems, 1L);
//        if (PATTERNS_FROM_EARLIER_WINDOWS && fisIxReader != null) {
//          Term term = new Term(ItemSetIndexBuilder.AssocField.ITEMSET.name, item);
//          TermDocs termDocs = fisIxReader.termDocs(term);
//          while (termDocs.next()) {
//            int docId = termDocs.doc();
//            Document doc = fisIxReader.document(docId);
//            TermFreqVector terms = fisIxReader.getTermFreqVector(docId,
//                ItemSetIndexBuilder.AssocField.ITEMSET.name);
//            
//            Set<Integer> appearingGroups = Sets.newHashSet();
//            IntArrayList pattern = new IntArrayList(terms.size());
//            for (String t : terms.getTerms()) {
//              if (fMap.containsKey(t)) {
//                int tId = fMap.get(t);
//                pattern.add(tId);
//                appearingGroups.add(PFPGrowth.getGroupHash(tId, numGroups));
//              } else {
//                context
//                    .setStatus("Parallel FPGrowth: Term from previous pattern not part of the current fList: "
//                        + t);
//              }
//            }
//            
//            float patternFreq = Float.parseFloat(doc
//                .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
//                .stringValue());
//            // patternFreq /= (j + 1);
//            // will be added that many times
//            patternFreq /= (terms.size() * appearingGroups.size());
//            long support = Math.round(timeWeigth.apply(patternFreq, mostRecentTime, intervalStart));
//            
//            patternTree.addPattern(pattern, support);
//          }
//        }
//        context.write(wGroupID, patternTree);
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
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP))); //GMT23JAN2011)));
    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE)));
    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize - 1);
    
    // int i = 0;
    // for (Pair<String, Long> e : PFPGrowth.readFList(context.getConfiguration())) {
    // fMap.put(e.getFirst(), i++);
    // }
    
    OpenIntHashSet usedIds = new OpenIntHashSet();
    OpenObjectLongHashMap<String> prevFLists = PFPGrowth.readOlderCachedFLists(context
        .getConfiguration(),
        intervalStart, TimeWeightFunction.getDefault(params));
    
    LinkedList<String> terms = Lists.newLinkedList();
    prevFLists.keysSortedByValue(terms);
    Iterator<String> termsIter = terms.descendingIterator();
    while (termsIter.hasNext()) {
      // featureReverseMap.add(e.getFirst());
      // freqList.add(e.getSecond());
      String t = termsIter.next();
      int id = Hashing.murmur3_32().hashString(t, Charset.forName("UTF-8")).asInt();
      int c = 0;
      while (usedIds.contains(id)) {
        // while(idToString.containsKey(id)){
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
      // idToString.put(id,t);
    }
    
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
    
    // splittReaderer = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN,
    // PFPGrowth.SPLITTER.toString()));
    
    // maxPerGroup = Integer.valueOf(params.getInt(PFPGrowth.MAX_PER_GROUP, 0));
    numGroups = params.getInt(PFPGrowth.NUM_GROUPS, PFPGrowth.NUM_GROUPS_DEFAULT);
    
    prependUserName = true;
    
//    if (PATTERNS_FROM_EARLIER_WINDOWS) {
//      Configuration conf = context.getConfiguration();
//      Path outPath = new Path(params.get(PFPGrowth.OUTPUT));
//      Path timeRoot = outPath.getParent().getParent();
//      FileSystem fs = FileSystem.get(conf);
//      FileStatus[] otherWindows = fs.listStatus(timeRoot);
//      mostRecentTime = Long.MIN_VALUE;
//      Path mostRecentPath = null;
//      for (int f = otherWindows.length - 1; f >= 0; --f) {
//        Path p = otherWindows[f].getPath();
//        long pathStartTime = Long.parseLong(p.getName());
//        // should have used end time, but it doesn't make a difference,
//        // AS LONG AS windows don't overlap
//        if (pathStartTime < intervalStart && pathStartTime > mostRecentTime) {
//          p = fs.listStatus(p)[0].getPath();
//          p = new Path(p, "index");
//          if (fs.exists(p)) {
//            mostRecentTime = pathStartTime;
//            mostRecentPath = p;
//          }
//        } else if (mostRecentPath != null) {
//          break;
//        }
//      }
//      if (mostRecentPath != null) {
//        File indexDir = FileUtils.toFile(mostRecentPath.toUri().toURL());
//        // FIXME: this will work only on local filesystem.. like many other parts of the code
//        Directory fisdir = new MMapDirectory(indexDir);
//        fisIxReader = IndexReader.open(fisdir);
//        // fisSearcher = new IndexSearcher(fisIxReader);
//        // fisSimilarity = new ItemSetSimilarity();
//        // fisSearcher.setSimilarity(fisSimilarity);
//        //
//        // fisQparser = new QueryParser(Version.LUCENE_36,
//        // ItemSetIndexBuilder.AssocField.ITEMSET.name,
//        // ANALYZER);
//        // fisQparser.setDefaultOperator(Operator.AND);
//        
//        timeWeigth = TimeWeightFunction.getDefault(params);
//      }
//    }
  }
}
