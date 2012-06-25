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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.list.IntArrayList;

/**
 * takes each group of dependent transactions and\ compacts it in a
 * TransactionTree structure
 */
public class ParallelFPStreamCombiner extends
    Reducer<IntWritable, TransactionTree, IntWritable, TransactionTree> {
  
//  private IndexReader fisIxReader;
//  private TimeWeightFunction timeWeigth;
//  private long mostRecentTime;
//  private long windowSize;
//  private long endTimestamp;
//  private long intervalStart;
//  private long intervalEnd;
//  
//  private final OpenIntObjectHashMap<String> idToString = new OpenIntObjectHashMap<String>();
//  private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
  
  @Override
  protected void reduce(IntWritable key, Iterable<TransactionTree> values, Context context)
      throws IOException, InterruptedException {
    TransactionTree cTree = new TransactionTree();
    for (TransactionTree tr : values) {
      for (Pair<IntArrayList, Long> p : tr) {
        cTree.addPattern(p.getFirst(), p.getSecond());
      }
//      if (fisIxReader != null) {
//        String item = idToString.get(key.get());
//        Term term = new Term(ItemSetIndexBuilder.AssocField.ITEMSET.name, item);
//        TermDocs termDocs = fisIxReader.termDocs(term);
//        while (termDocs.next()) {
//          int docId = termDocs.doc();
//          Document doc = fisIxReader.document(docId);
//          TermFreqVector terms = fisIxReader.getTermFreqVector(docId,
//              ItemSetIndexBuilder.AssocField.ITEMSET.name);
//          
//          float patternFreq = Float.parseFloat(doc
//              .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
//              .stringValue());
//          patternFreq /= terms.size(); // (j + 1);
//          long support = Math.round(timeWeigth.apply(patternFreq, mostRecentTime, intervalStart));
//          
//          IntArrayList pattern = new IntArrayList(terms.size());
//          for (String t : terms.getTerms()) {
//            if (fMap.containsKey(t)) {
//              pattern.add(fMap.get(t));
//            } else {
//              context
//                  .setStatus("Parallel FPGrowth: Term from previous pattern not part of the current fList: "
//                      + t);
//            }
//          }
//          cTree.addPattern(pattern, support);
//        }
//      }
    }
    context.write(key, cTree.getCompressedTree());
  }
  
//  protected void setup(
//      org.apache.hadoop.mapreduce.Reducer<IntWritable, TransactionTree, IntWritable, TransactionTree>.Context context)
//      throws IOException, InterruptedException {
//    super.setup(context);
//    Parameters params =
//        new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
//    
//    intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
//    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP))); //GMT23JAN2011)));
//    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
//    // Long.toString(Long.MAX_VALUE)));
//    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
//        Long.toString(intervalEnd - intervalStart)));
//    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize - 1);
//    
//    Configuration conf = context.getConfiguration();
//    OpenObjectLongHashMap<String> prevFLists = PFPGrowth.readOlderCachedFLists(conf,
//        intervalStart, TimeWeightFunction.getDefault(params));
//    
//    LinkedList<String> terms = Lists.newLinkedList();
//    prevFLists.keysSortedByValue(terms);
//    Iterator<String> termsIter = terms.descendingIterator();
//    while (termsIter.hasNext()) {
//      // featureReverseMap.add(e.getFirst());
//      // freqList.add(e.getSecond());
//      String t = termsIter.next();
//      int id = Hashing.murmur3_32().hashString(t, Charset.forName("UTF-8")).asInt();
//      int c = 0;
//      while (idToString.containsKey(id)) {
//        // while(idToString.containsKey(id)){
//        // Best effort
//        if (c < t.length()) {
//          id = Hashing.murmur3_32((int) t.charAt(c++)).hashString(t, Charset.forName("UTF-8"))
//              .asInt();
//        } else {
//          ++id;
//        }
//      }
//      fMap.put(t, id);
//      idToString.put(id, t);
//    }
//    
//    Path outPath = new Path(params.get(PFPGrowth.OUTPUT));
//    Path timeRoot = outPath.getParent().getParent();
//    FileSystem fs = FileSystem.get(conf);
//    FileStatus[] otherWindows = fs.listStatus(timeRoot);
//    mostRecentTime = Long.MIN_VALUE;
//    Path mostRecentPath = null;
//    for (int f = otherWindows.length - 1; f >= 0; --f) {
//      Path p = otherWindows[f].getPath();
//      long pathStartTime = Long.parseLong(p.getName());
//      // should have used end time, but it doesn't make a difference,
//      // AS LONG AS windows don't overlap
//      if (pathStartTime < intervalStart && pathStartTime > mostRecentTime) {
//        p = fs.listStatus(p)[0].getPath();
//        p = new Path(p, "index");
//        if (fs.exists(p)) {
//          mostRecentTime = pathStartTime;
//          mostRecentPath = p;
//        }
//      } else if (mostRecentPath != null) {
//        break;
//      }
//    }
//    if (mostRecentPath != null) {
//      File indexDir = FileUtils.toFile(mostRecentPath.toUri().toURL());
//      // FIXME: this will work only on local filesystem.. like many other parts of the code
//      Directory fisdir = new MMapDirectory(indexDir);
//      fisIxReader = IndexReader.open(fisdir);
//      // fisSearcher = new IndexSearcher(fisIxReader);
//      // fisSimilarity = new ItemSetSimilarity();
//      // fisSearcher.setSimilarity(fisSimilarity);
//      //
//      // fisQparser = new QueryParser(Version.LUCENE_36,
//      // ItemSetIndexBuilder.AssocField.ITEMSET.name,
//      // ANALYZER);
//      // fisQparser.setDefaultOperator(Operator.AND);
//      
//      timeWeigth = TimeWeightFunction.getDefault(params);
//    }
//  }
}
