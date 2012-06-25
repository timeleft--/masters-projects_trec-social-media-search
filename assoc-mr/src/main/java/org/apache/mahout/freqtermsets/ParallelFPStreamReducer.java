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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.convertors.ContextStatusUpdater;
import org.apache.mahout.freqtermsets.convertors.ContextWriteOutputCollector;
import org.apache.mahout.freqtermsets.convertors.integer.IntegerStringOutputConverter;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.freqtermsets.fpgrowth.FPGrowth;
import org.apache.mahout.freqtermsets.stream.TimeWeightFunction;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.map.OpenObjectLongHashMap;

import ca.uwaterloo.twitter.ItemSetIndexBuilder;
import ca.uwaterloo.twitter.ItemSetSimilarity;
import ca.uwaterloo.twitter.TokenIterator;
import ca.uwaterloo.twitter.TwitterAnalyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

/**
 * takes each group of transactions and runs Vanilla FPGrowth on it and
 * outputs the the Top K frequent Patterns for each group.
 * 
 */
public class ParallelFPStreamReducer extends
    Reducer<IntWritable, TransactionTree, Text, TopKStringPatterns> {
  
  class OldPatternsCollector extends Collector {
    private int docBase;
    private IndexReader reader;
    private Set<Set<String>> encounteredDocs = Sets.newHashSet();
    private final TaskInputOutputContext<?, ?, ?, ?> context;
    private final TransactionTree extendedTree;
    
    private OldPatternsCollector(
        TaskInputOutputContext<?, ?, ?, ?> context,
        TransactionTree extendedTree) {
      super();
      this.context = context;
      this.extendedTree = extendedTree;
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }
    
    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      this.docBase = docBase;
      this.reader = reader;
    }
    
    @Override
    public void collect(int docId) throws IOException {
      Document doc = reader.document(docId); // +docBase??
      String[] terms = reader.getTermFreqVector(docId,
          ItemSetIndexBuilder.AssocField.ITEMSET.name).getTerms();
      Set<String> termSet = Sets.newCopyOnWriteArraySet(Arrays.asList(terms));
      if (encounteredDocs.contains(termSet)) {
        return;
      } else {
        encounteredDocs.add(termSet);
      }
      
      // Set<Integer> appearingGroups = Sets.newHashSet();
      IntArrayList pattern = new IntArrayList(terms.length);
      for (String t : terms) {
        if (stringIdMap.containsKey(t)) {
          int tId = stringIdMap.get(t);
          pattern.add(tId);
          // appearingGroups.add(PFPGrowth.getGroupHash(tId, numGroups));
        } else {
          context
              .setStatus("Parallel FPGrowth: Term from previous pattern not part of the current fList: "
                  + t);
        }
      }
      
      float patternFreq = Float.parseFloat(doc
          .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
          .stringValue());
      // No need to divide because originally every transaction was added once per group
      // // will be added that many times
      // patternFreq /= appearingGroups.size();
      long support = Math.round(timeWeigth.apply(patternFreq, mostRecentTime, intervalStart));
      
      extendedTree.addPattern(pattern, support);
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }
  
  private IndexReader fisIxReader;
  private IndexSearcher fisSearcher;
  private ItemSetSimilarity fisSimilarity;
  private QueryParser fisQparser;
  private Analyzer ANALYZER = new TwitterAnalyzer();
  
  public static final String MIN_WORDS_FOR_LANG_ID = "lenLangId";
  public static final int MIN_WORDS_FOR_LANG_ID_DEFAULT = 3;
  
  private final OpenIntObjectHashMap<String> idStringMap = new OpenIntObjectHashMap<String>();
  private final OpenObjectIntHashMap<String> stringIdMap = new OpenObjectIntHashMap<String>();
  
  private TimeWeightFunction timeWeigth;
  private long mostRecentTime;
  
  private int maxHeapSize = 50;
  
  private int minSupport = 3;
  
  private int numGroups;
  
  private int minWordsForLangDetection;
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
      return new Pair<List<Integer>, Long>(innerNext.getFirst().toList(), innerNext.getSecond());
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
    
    if (fisIxReader != null) {
      BooleanQuery allPatternsQuery = new BooleanQuery();
      Iterator<Pair<IntArrayList, Long>> cTreeIter = cTree.iterator(false);
      while (cTreeIter.hasNext()) {
        IntArrayList newPatternIds = cTreeIter.next().getFirst();
        if (newPatternIds.size() == 1) {
          // This is already carried over by loading the older flists
          continue;
        }
        StringBuilder newPattenStr = new StringBuilder();
        for (int i = 0; i < newPatternIds.size(); ++i) {
          int id = newPatternIds.getQuick(i);
          String str = idStringMap.get(id);
          newPattenStr.append(str).append(" ");
        }
        try {
          allPatternsQuery.add(fisQparser.parse(newPattenStr.toString()), Occur.SHOULD);
          // fisSearcher.search(fisQparser.parse(newPattenStr.toString()),oldPatternsCollector);
        } catch (ParseException e) {
          context.setStatus("Parallel FPGrowth: caught a parse exception: " + e.getMessage());
          continue;
        }
      }
      
      fisSearcher.search(allPatternsQuery, new OldPatternsCollector(context, cTree));
      
    }
    
    List<Pair<Integer, Long>> localFList = Lists.newArrayList();
    for (Entry<Integer, MutableLong> fItem : cTree.generateFList().entrySet()) {
      localFList.add(new Pair<Integer, Long>(fItem.getKey(), fItem.getValue().toLong()));
    }
    
    Collections.sort(localFList, new CountDescendingPairComparator<Integer, Long>());
    
    FPGrowth<Integer> fpGrowth = new FPGrowth<Integer>();
    fpGrowth
        .generateTopKFrequentPatterns(
            new IteratorAdapter(cTree.iterator()),
            localFList,
            minSupport,
            maxHeapSize,
            null,
            new IntegerStringOutputConverter(
                new ContextWriteOutputCollector<IntWritable, TransactionTree, Text, TopKStringPatterns>(
                    context),
                idStringMap,
                minWordsForLangDetection, repeatHashTag),
            new ContextStatusUpdater<IntWritable, TransactionTree, Text, TopKStringPatterns>(
                context),
            key.get(),
            numGroups);
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    
    super.setup(context);
    Configuration conf = context.getConfiguration();
    Parameters params = new Parameters(conf.get(PFPGrowth.PFP_PARAMETERS, ""));
    
    intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize - 1);
    
    PFPGrowth.loadEarlierFlists(context, params, intervalStart, idStringMap, stringIdMap);
    
    maxHeapSize = Integer.valueOf(params.get(PFPGrowth.MAX_HEAPSIZE, "50"));
    minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
    
    numGroups = params.getInt(PFPGrowth.NUM_GROUPS, PFPGrowth.NUM_GROUPS_DEFAULT);
    
    minWordsForLangDetection = params.getInt(MIN_WORDS_FOR_LANG_ID, MIN_WORDS_FOR_LANG_ID_DEFAULT);
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
    
    Path outPath = new Path(params.get(PFPGrowth.OUTPUT));
    Path timeRoot = outPath.getParent().getParent();
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] otherWindows = fs.listStatus(timeRoot);
    mostRecentTime = Long.MIN_VALUE;
    Path mostRecentPath = null;
    for (int f = otherWindows.length - 1; f >= 0; --f) {
      Path p = otherWindows[f].getPath();
      long pathStartTime = Long.parseLong(p.getName());
      // should have used end time, but it doesn't make a difference,
      // AS LONG AS windows don't overlap
      if (pathStartTime < intervalStart && pathStartTime > mostRecentTime) {
        p = fs.listStatus(p)[0].getPath();
        p = new Path(p, "index");
        if (fs.exists(p)) {
          mostRecentTime = pathStartTime;
          mostRecentPath = p;
        }
      } else if (mostRecentPath != null) {
        break;
      }
    }
    if (mostRecentPath != null) {
      File indexDir = FileUtils.toFile(mostRecentPath.toUri().toURL());
      // FIXME: this will work only on local filesystem.. like many other parts of the code
      Directory fisdir = new MMapDirectory(indexDir);
      fisIxReader = IndexReader.open(fisdir);
      fisSearcher = new IndexSearcher(fisIxReader);
      fisSimilarity = new ItemSetSimilarity();
      fisSearcher.setSimilarity(fisSimilarity);
      
      fisQparser = new QueryParser(Version.LUCENE_36,
          ItemSetIndexBuilder.AssocField.ITEMSET.name,
          ANALYZER);
      fisQparser.setDefaultOperator(Operator.AND);
      
      timeWeigth = TimeWeightFunction.getDefault(params);
    }
  }
}
