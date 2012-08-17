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
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.freqtermsets.fpgrowth.FPGrowth;
import org.apache.mahout.freqtermsets.stream.TimeWeightFunction;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.map.OpenObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.twitter.corpus.data.CSVTweetInputFormat;
import com.twitter.corpus.data.util.PartitionByTimestamp;

/**
 * 
 * Parallel FP Growth Driver Class. Runs each stage of PFPGrowth as described in the paper
 * http://infolab.stanford.edu/~echang/recsys08-69.pdf
 * 
 */
public final class PFPGrowth implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(PFPGrowth.class);
  
  public static final String ENCODING = "encoding";
  public static final String F_LIST = "fList";
  public static final String G_LIST = "gList";
  public static final String NUM_GROUPS = "numGroups";
  public static final int NUM_GROUPS_DEFAULT = 1000;
  public static final String MAX_PER_GROUP = "maxPerGroup";
  public static final String OUTPUT = "output";
  public static final String OUTROOT = "outroot";
  public static final String MIN_SUPPORT = "minSupport";
  public static final String MAX_HEAPSIZE = "maxHeapSize";
  public static final String INPUT = "input";
  public static final String PFP_PARAMETERS = "pfp.parameters";
  public static final String FILE_PATTERN = "part-*";
  public static final String FPGROWTH = "fpgrowth";
  public static final String FREQUENT_PATTERNS = "frequentpatterns";
  public static final String PARALLEL_COUNTING = "parallelcounting";
  
  // public static final String USE_FPG2 = "use_fpg2";
  // YA
  public static final String PRUNE_PCTILE = "percentile";
  public static final int PRUNE_PCTILE_DEFAULT = 95;
  public static final String MIN_FREQ = "minFreq";
  public static final int MIN_FREQ_DEFAULT = 33;
  // All those setting are cluster level and cannot be set per job
  // public static final String PSEUDO = "pseudo";
  public static final String COUNT_IN = "countIn";
  public static final String GROUP_FIS_IN = "gfisIn";
  public static final String PARAM_INTERVAL_START = "startTime";
  public static final String PARAM_INTERVAL_END = "endTime";
  // public static final String INDEX_OUT = "index";
  
  public static final String PARAM_WINDOW_SIZE = "windowSize";
  public static final String PARAM_STEP_SIZE = "stepSize";
  
  // TODO command line
  // static final boolean FPSTREAM = false;
  public static enum RunningMode {
    Batch, BlockUpdate, SlidingWin
  };
  
  public static final RunningMode runMode = RunningMode.SlidingWin;
  
  public static final float FPSTREAM_LINEAR_DECAY_COEFF = 0.9f;
  // private static final double AVG_TOKENS_PER_DOC = 7;
  public static final String PARAM_MAX_PATTERN_LOAD_LAG = "maxLag";
  public static final String DEFAULT_MAX_PATTERN_LOAD_LAG = Long.toString(24 * 3600 * 1000);
  
  // private static final long DIRICHLET_SMOOTHING_PARAM = 3579L;
  
  // public static final long TREC2011_MIN_TIMESTAMP = 1296130141000L; // 1297209010000L;
  // public static final long GMT23JAN2011 = 1295740800000L;
  
  // Not text input anymore
  // public static final String SPLIT_PATTERN = "splitPattern";
  // public static final Pattern SPLITTER = Pattern.compile("[ ,\t]*[,|\t][ ,\t]*");
  // END YA
  // private PFPGrowth() {
  // }
  
  public static void loadEarlierFHashMaps(JobContext context, Parameters params,
      long intervalStart,
      OpenIntObjectHashMap<String> idStringMapOut, OpenObjectIntHashMap<String> stringIdMapOut)
      throws IOException {
    // I resist the urge to cache this list because I don't know what exactly would happen
    // when the job is run in hadoop where every job has its own JVM.. will static
    // fields somehow leak? Can I be sure that the static WeakHashMap used as a cache is mine?
    // FINALLY.. the list would be loaded only twice, once for mapper, and once for reducer
    
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
      while (idStringMapOut.containsKey(id)) {
        // Best effort
        if (c < t.length()) {
          id = Hashing.murmur3_32((int) t.charAt(c++)).hashString(t, Charset.forName("UTF-8"))
              .asInt();
        } else {
          ++id;
        }
      }
      
      idStringMapOut.put(id, t);
      stringIdMapOut.put(t, id);
    }
  }
  
  public static long readTermFreqHashMap(Configuration conf, OpenObjectLongHashMap<String> fMap)
      throws IOException {
    
    long totalNterms = 0;
    
    switch (runMode) {
    case Batch:
    case SlidingWin:
      for (Pair<String, Long> e : readCachedFList(conf)) {
        fMap.put(e.getFirst(), e.getSecond());
        totalNterms += e.getSecond();
      }
      break;
    case BlockUpdate:
      
      Parameters params = new Parameters(conf.get(PFPGrowth.PFP_PARAMETERS, ""));
      long currWindowStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
      
      OpenObjectLongHashMap<String> prevFLists = readOlderCachedFLists(conf,
          currWindowStart,
          TimeWeightFunction.getDefault(params));
      LinkedList<String> terms = Lists.newLinkedList();
      prevFLists.keysSortedByValue(terms);
      Iterator<String> termsIter = terms.descendingIterator();
      
      while (termsIter.hasNext()) {
        String t = termsIter.next();
        long freq = prevFLists.get(t);
        fMap.put(t, freq);
        totalNterms += freq;
      }
      break;
    }
    return totalNterms;
  }
  
  // public static List<Pair<String, Long>> readFList(Configuration conf) throws IOException {
  // if (runMode.equals(RunningMode.Batch)) {
  //
  // return readCachedFList(conf);
  //
  // } else {
  //
  // Parameters params = new Parameters(conf.get(PFPGrowth.PFP_PARAMETERS, ""));
  // long currWindowStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
  //
  // OpenObjectLongHashMap<String> prevFLists = readOlderCachedFLists(conf,
  // currWindowStart,
  // TimeWeightFunction.getDefault(params));
  // LinkedList<String> terms = Lists.newLinkedList();
  // prevFLists.keysSortedByValue(terms);
  // Iterator<String> termsIter = terms.descendingIterator();
  //
  // List<Pair<String, Long>> result =
  // Lists.<Pair<String, Long>> newArrayListWithCapacity(terms.size());
  // while (termsIter.hasNext()) {
  // String t = termsIter.next();
  // result.add(new Pair<String, Long>(t, prevFLists.get(t)));
  // }
  //
  // return result;
  // }
  // }
  //
  /**
   * Generates the fList from the serialized string representation
   * 
   * @return Deserialized Feature Frequency List
   */
  public static OpenObjectLongHashMap<String> readOlderCachedFLists(Configuration conf,
      long currWindowStart, TimeWeightFunction weightFunction) throws IOException {
    OpenObjectLongHashMap<String> list = new OpenObjectLongHashMap<String>();
    Path[] files = DistributedCache.getLocalCacheFiles(conf);
    if (files == null) {
      throw new IOException("Cannot read Frequency list from Distributed Cache");
    }
    for (int i = 0; i < files.length; ++i) {
      FileSystem fs = FileSystem.getLocal(conf);
      Path fListLocalPath = fs.makeQualified(files[i]);
      // Fallback if we are running locally.
      if (!fs.exists(fListLocalPath)) {
        URI[] filesURIs = DistributedCache.getCacheFiles(conf);
        if (filesURIs == null) {
          throw new IOException("Cannot read Frequency list from Distributed Cache");
        }
        fListLocalPath = new Path(filesURIs[i].getPath());
      }
      long listWindowStart = Long.parseLong(fListLocalPath.getParent().getParent().getName());
      for (Pair<Text, LongWritable> record : new SequenceFileIterable<Text, LongWritable>(
          fListLocalPath, true, conf)) {
        String token = record.getFirst().toString();
        
        list.put(token,
            Math.round(list.get(token)
                + weightFunction.apply(record.getSecond().get(), listWindowStart, currWindowStart)));
      }
    }
    return list;
  }
  
  /**
   * Generates the fList from the serialized string representation
   * 
   * @return Deserialized Feature Frequency List
   */
  public static List<Pair<String, Long>> readCachedFList(Configuration conf) throws IOException {
    List<Pair<String, Long>> list = new ArrayList<Pair<String, Long>>();
    Path[] files = DistributedCache.getLocalCacheFiles(conf);
    if (files == null) {
      throw new IOException("Cannot read Frequency list from Distributed Cache");
    }
    if (files.length != 1) {
      throw new IOException("Cannot read Frequency list from Distributed Cache (" + files.length
          + ")");
    }
    FileSystem fs = FileSystem.getLocal(conf);
    Path fListLocalPath = fs.makeQualified(files[0]);
    // Fallback if we are running locally.
    if (!fs.exists(fListLocalPath)) {
      URI[] filesURIs = DistributedCache.getCacheFiles(conf);
      if (filesURIs == null) {
        throw new IOException("Cannot read Frequency list from Distributed Cache");
      }
      if (filesURIs.length != 1) {
        throw new IOException("Cannot read Frequency list from Distributed Cache (" + files.length
            + ")");
      }
      fListLocalPath = new Path(filesURIs[0].getPath());
    }
    // Done below, while caching the list
    // // YA: Lang independent stop words removal
    // // FIXMENOT: as below
    // Parameters params = new Parameters(conf.get(PFP_PARAMETERS, ""));
    // int minFr = params.getInt(MIN_FREQ, MIN_FREQ_DEFAULT);
    // int prunePct = params.getInt(PRUNE_PCTILE, PRUNE_PCTILE_DEFAULT);
    //
    // // TODONOT: assert minFr >= minSupport;
    //
    // Iterator<Pair<Text, LongWritable>> tempIter = new SequenceFileIterable<Text, LongWritable>(
    // fListLocalPath, true, conf).iterator();
    // long maxFr = Long.MAX_VALUE;
    // if (tempIter.hasNext()) {
    // maxFr = tempIter.next().getSecond().get() * prunePct / 100;
    // }
    // tempIter = null;
    //
    // for (Pair<Text, LongWritable> record : new SequenceFileIterable<Text, LongWritable>(
    // fListLocalPath, true, conf)) {
    // String token = record.getFirst().toString();
    // char ch0 = token.charAt(0);
    // if ((ch0 != '#' && ch0 != '@')
    // && (record.getSecond().get() < minFr || record.getSecond().get() > maxFr)) {
    // continue;
    // }
    // list.add(new Pair<String, Long>(token, record.getSecond().get()));
    // }
    // // END YA
    
    for (Pair<Text, LongWritable> record : new SequenceFileIterable<Text, LongWritable>(
        fListLocalPath, true, conf)) {
      list.add(new Pair<String, Long>(record.getFirst().toString(), record.getSecond().get()));
    }
    
    return list;
  }
  
  /**
   * Serializes the fList and returns the string representation of the List
   * 
   * @param flistPath
   * 
   * @return Serialized String representation of List
   */
  public static void saveFList(Iterable<Pair<String, Long>> flist, // Parameters params,
      Configuration conf, Path flistPath)
      throws IOException {
    FileSystem fs = FileSystem.get(flistPath.toUri(), conf);
    flistPath = fs.makeQualified(flistPath);
    // HadoopUtil.delete(conf, flistPath);
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, flistPath, Text.class,
        LongWritable.class);
    try {
      for (Pair<String, Long> pair : flist) {
        writer.append(new Text(pair.getFirst()), new LongWritable(pair.getSecond()));
      }
    } finally {
      writer.close();
    }
    DistributedCache.addCacheFile(flistPath.toUri(), conf);
  }
  
  // /**
  // * read the feature frequency List which is built at the end of the Parallel counting job
  // *
  // * @param countIn
  // * @param minSupport
  // * @param minFr
  // * @param prunePct
  // *
  // * @return Feature Frequency List
  // */
  // public static List<Pair<String, Long>> readFList(String countIn, int minSupport, int minFr,
  // int prunePct) throws IOException {
  //
  // Path parallelCountingPath = new Path(countIn, PARALLEL_COUNTING);
  //
  // PriorityQueue<Pair<String, Long>> queue = new PriorityQueue<Pair<String, Long>>(11,
  // new Comparator<Pair<String, Long>>() {
  // @Override
  // public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
  // int ret = o2.getSecond().compareTo(o1.getSecond());
  // if (ret != 0) {
  // return ret;
  // }
  // return o1.getFirst().compareTo(o2.getFirst());
  // }
  // });
  //
  // Path path = new Path(parallelCountingPath, FILE_PATTERN);
  // // if(!FileSystem.get(path.toUri(),conf).exists(path)){
  // // throw new IOException("Cannot find flist file: " + path);
  // // }
  // Configuration conf = new Configuration();
  // // double totalFreq = 0;
  // for (Pair<Text, LongWritable> record : new SequenceFileDirIterable<Text, LongWritable>(
  // path, PathType.GLOB, null, null, true, conf)) {
  // long freq = record.getSecond().get();
  // // totalFreq += freq;
  // queue.add(new Pair<String, Long>(record.getFirst().toString(), freq));
  // }
  // //
  // // YA: language indipendent stop words.. the 5% most frequent
  // // FIXME: this will remove words from only the mostly used lang
  // // i.e. cannot be used for a multilingual task
  // long maxFreq = Long.MAX_VALUE;
  // if (!queue.isEmpty()) {
  // maxFreq = Math.round(1.0f * queue.peek().getSecond() * prunePct / 100);
  // // maxFreq = Math.round(1.0f * queue.size() * prunePct / 100);
  // }
  // // totalFreq += DIRICHLET_SMOOTHING_PARAM;
  // // double pruneProb = prunePct / 100.0;
  // assert minFr >= minSupport;
  // // double numDocs = totalFreq / AVG_TOKENS_PER_DOC;
  //
  // List<Pair<String, Long>> fList = Lists.newArrayList();
  // boolean withinUpperBound = false;
  // // int i=0;
  // while (!queue.isEmpty()) {
  // Pair<String, Long> record = queue.poll();
  // String token = record.getFirst().toString();
  // char ch0 = token.charAt(0);
  // long value = record.getSecond();
  // // Always Count patterns associated with hashtags and mentions,
  // // but for other tokens ONLY those who are within medium range
  // // double prob = (value + DIRICHLET_SMOOTHING_PARAM) / totalFreq;
  // if (!withinUpperBound) {
  // // double idf = MathUtils.log(2, numDocs / value+1) + 1;
  // // withinUpperBound = idf >= prunePct/* value <= maxFreq */;
  // // withinUpperBound = (value/totalFreq) < pruneProb;
  // withinUpperBound = value <= maxFreq;
  // // withinUpperBound = i > maxFreq;
  // // ++i;
  // }
  // if (!(value >= minFr)) { // (withinLoweBound){
  // break;
  // }
  // if ((ch0 == '#' || ch0 == '@')
  // || (withinUpperBound)) { // && withinLoweBound)) {
  // fList.add(record);
  // }
  // }
  // return fList; // This prevents a null exception when using FP2 --> .subList(0, fList.size());
  // }
  
  public static void appendParallelCountingResults(String countIn,
      Configuration conf, OpenObjectLongHashMap<String> freqMapOut) throws IOException {
    
    Path parallelCountingPath = new Path(countIn, PARALLEL_COUNTING);
    
    Path path = new Path(parallelCountingPath, FILE_PATTERN);
    
    for (Pair<Text, LongWritable> record : new SequenceFileDirIterable<Text, LongWritable>(
        path, PathType.GLOB, null, null, true, conf)) {
      long freq = record.getSecond().get();
      String token = record.getFirst().toString();
      
      freqMapOut.put(token, freqMapOut.get(token) + freq);
    }
    
  }
  
  /**
   * read the feature frequency List which is built at the end of the Parallel counting job
   * 
   * @param countIn
   * 
   * @return Feature Frequency Map
   */
  public static OpenObjectLongHashMap<String> readParallelCountingResults(String countIn,
      Configuration conf) throws IOException {
    
    OpenObjectLongHashMap<String> result = new OpenObjectLongHashMap<String>();
    Path parallelCountingPath = new Path(countIn, PARALLEL_COUNTING);
    
    Path path = new Path(parallelCountingPath, FILE_PATTERN);
    
    for (Pair<Text, LongWritable> record : new SequenceFileDirIterable<Text, LongWritable>(
        path, PathType.GLOB, null, null, true, conf)) {
      long freq = record.getSecond().get();
      String token = record.getFirst().toString();
      
      result.put(token, freq);
    }
    
    return result;
  }
  
  public static List<Pair<String, Long>> pruneParallelCountingResults(
      OpenObjectLongHashMap<String> freqMap,
      int minSupport, int minFr, int prunePct) throws IOException {
    
    List<String> termsSorted = Lists.newArrayListWithCapacity(freqMap.size());
    freqMap.keysSortedByValue(termsSorted);
    
    // FIXME: this will remove words from only the mostly used lang
    // i.e. cannot be used for a multilingual task
    // Percentile: Pretty aggressive since the Zipfe distribution is very steep
    int maxIx = (int) Math.round(((freqMap.size() - 1) * prunePct / 100.0) + 1);
    long maxFreq = freqMap.get(termsSorted.get(maxIx));
    // Alternatives
    // double maxFreq = (1.0f * MathUtils.log(2,freqArr[0].getSecond()) * prunePct / 100);
    // double maxFreq = (1.0f * totalFreq * prunePct / 100); //FIX this to be weighted percentile
    
    // This will ALWAYS prune something; even the 100th percentile (i.e. highest rank)
    while (freqMap.get(termsSorted.get(maxIx - 1)) == maxFreq) {
      --maxIx;
    }
    
    assert minFr >= minSupport;
    int minIx = 0;
    while (freqMap.get(termsSorted.get(minIx)) < minFr) {
      ++minIx;
    }
    List<Pair<String, Long>> result = Lists.newArrayListWithExpectedSize(maxIx - minIx);
    
    for (int i = maxIx - 1; i >= minIx; --i) {
      String term = termsSorted.get(i);
      result.add(new Pair(term, freqMap.get(term)));
    }
    
    return result;
  }
  
  // public static List<Pair<String, Long>> readParallelCountingResults(String countIn, int
  // minSupport, int minFr,
  // int prunePct, Configuration conf) throws IOException {
  //
  // Path parallelCountingPath = new Path(countIn, PARALLEL_COUNTING);
  //
  // Path path = new Path(parallelCountingPath, FILE_PATTERN);
  // // = new Configuration();
  // // if (!FileSystem.get(path.toUri(), conf).exists(path)) {
  // // throw new IOException("Cannot find flist file: " + path);
  // // }
  //
  // // Won't hold in case of steps
  // assert minFr >= minSupport;
  // double totalFreq = 0;
  // List<Pair<String, Long>> freqList = Lists.newLinkedList();
  // for (Pair<Text, LongWritable> record : new SequenceFileDirIterable<Text, LongWritable>(
  // path, PathType.GLOB, null, null, true, conf)) {
  // long freq = record.getSecond().get();
  // String token = record.getFirst().toString();
  //
  // // char ch0 = token.charAt(0);
  // // No special treatment for mentions: || ch0 == '@'
  // // or hashtags: (ch0 == '#') ||
  // if ((freq >= minFr)) {
  // freqList.add(new Pair<String, Long>(token, freq));
  // totalFreq += freq;
  // }
  // }
  //
  // Comparator<Pair<String, Long>> descComp =
  // new Comparator<Pair<String, Long>>() {
  // @Override
  // public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
  // int ret = o2.getSecond().compareTo(o1.getSecond());
  // if (ret != 0) {
  // return ret;
  // }
  // return o1.getFirst().compareTo(o2.getFirst());
  // }
  // };
  //
  // @SuppressWarnings("unchecked")
  // Pair<String, Long>[] freqArr = freqList.toArray(new Pair[0]);
  // Arrays.sort(freqArr, descComp);
  //
  // List<Pair<String, Long>> result = null; // = Lists.newLinkedList();
  // // YA: language indipendent stop words.. the 5% most frequent
  // // FIXME: this will remove words from only the mostly used lang
  // // i.e. cannot be used for a multilingual task
  // // Percentile: Pretty aggressive since the Zipfe distribution is very steep
  // int minIx = (int) Math.round(1.0f * (freqArr.length + 1) * (100 - prunePct) / 100);
  // long maxFreq = freqArr[minIx].getSecond();
  // // double maxFreq = (1.0f * MathUtils.log(2,freqArr[0].getSecond()) * prunePct / 100);
  // // double maxFreq = (1.0f * totalFreq * prunePct / 100);
  // boolean withinUpperBound = false;
  // for (int i = 0; i < freqArr.length; ++i) {
  // if (!withinUpperBound) {
  // // totalFreq -= freqArr[i].getSecond();
  // // withinUpperBound = totalFreq <= maxFreq;
  // // withinUpperBound = MathUtils.log(2,freqArr[i].getSecond()) <= maxFreq;
  // withinUpperBound = freqArr[i].getSecond() <= maxFreq;
  // if (withinUpperBound) {
  // result = Lists.newArrayListWithCapacity(freqArr.length - i);
  // }
  // }
  //
  // if (withinUpperBound) {
  // result.add(freqArr[i]);
  // }
  // }
  //
  // return result;
  // }
  //
  public static int cacheFList(Parameters params,
      Configuration conf, String countIn,
      int minSupport, int minFr, int prunePct) throws IOException {
    
    long startTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(endTime - startTime)));
    long stepSize = Long
        .parseLong(params.get(PFPGrowth.PARAM_STEP_SIZE, Long.toString(windowSize)));
    endTime = Math.min(endTime, startTime + windowSize);
    
    Path cachedPath = new Path(countIn, Long.toString(startTime));
    cachedPath = new Path(cachedPath, Long.toString(Math.min(endTime, startTime + windowSize)));
    cachedPath = new Path(cachedPath, F_LIST);
    
    FileSystem fs = FileSystem.getLocal(conf);
    int result;
    if (fs.exists(cachedPath)) {
      // assert FPSTREAM;
      result = -1;
      DistributedCache.addCacheFile(cachedPath.toUri(), conf);
    } else {
      OpenObjectLongHashMap<String> freqMap;
      if (runMode.equals(RunningMode.SlidingWin)) {
        freqMap = new OpenObjectLongHashMap<String>();
        while (startTime < endTime) {
          String stepCount = FilenameUtils.concat(countIn, Long.toString(startTime));
          stepCount = FilenameUtils.concat(stepCount,
              Long.toString(Math.min(endTime, startTime + stepSize)));
          
          appendParallelCountingResults(stepCount, conf, freqMap);
          startTime += stepSize;
        }
      } else {
        countIn = FilenameUtils.concat(countIn, Long.toString(startTime));
        countIn = FilenameUtils.concat(countIn,
            Long.toString(Math.min(endTime, startTime + windowSize)));
        freqMap = readParallelCountingResults(countIn, conf);
      }
      List<Pair<String, Long>> flist = pruneParallelCountingResults(
          freqMap,
          minSupport,
          minFr,
          prunePct);
      saveFList(flist, // params,
          conf,
          cachedPath);
      result = flist.size();
    }
    return result;
  }
  
  public static int getGroup(int itemId, int maxPerGroup) {
    return itemId / maxPerGroup;
  }
  
  public static IntArrayList getGroupMembers(int groupId,
      int maxPerGroup,
      int numFeatures) {
    IntArrayList ret = new IntArrayList();
    int start = groupId * maxPerGroup;
    int end = start + maxPerGroup;
    if (end > numFeatures)
      end = numFeatures;
    for (int i = start; i < end; i++) {
      ret.add(i);
    }
    return ret;
  }
  
  public static int getGroupFromHash(int attrHash, int numGroups) {
    // TODO save log(2) as a constant and devide by it.. this function is SLOW
    int maskLen = (int) MathUtils.log(2, numGroups - 1) + 1;
    int mask = (int) Math.pow(2, maskLen) - 1;
    int result = 0;
    
    int attrLSBs = 0;
    int byteMask = 255;
    int numBytes = (maskLen / 8) + 1;
    
    int leftShifts = 0;
    while (leftShifts < maskLen) { // No folding.. the murmer hash seems to be a repeating: 32) {
    
      for (int i = 0; i < numBytes; ++i) {
        attrLSBs += attrHash & byteMask;
        byteMask <<= 8;
      }
      result ^= attrLSBs & mask;
      
      leftShifts += maskLen;
      attrHash >>>= maskLen;
    }
    
    return result + 1; // group numbers are not zero based
  }
  
  // Modulo doesn't work because the hash is not necessarily positive
  // public static int getGroupFromHash(int attrHash, int numGroups) {
  // int result = (attrHash % numGroups) + 1;
  // return result;
  // }
  
  public static boolean isGroupMember(int groupId, int attrId, int numGroups) {
    int attrGroup = getGroupFromHash(attrId, numGroups);
    return groupId == attrGroup;
  }
  
  /**
   * Read the Frequent Patterns generated from Text
   * 
   * @return List of TopK patterns for each string frequent feature
   */
  public static List<Pair<String, TopKStringPatterns>> readFrequentPattern(Parameters params)
      throws IOException {
    
    Configuration conf = new Configuration();
    
    Path frequentPatternsPath = new Path(params.get(OUTPUT), FREQUENT_PATTERNS);
    FileSystem fs = FileSystem.get(frequentPatternsPath.toUri(), conf);
    FileStatus[] outputFiles = fs.globStatus(new Path(frequentPatternsPath, FILE_PATTERN));
    
    List<Pair<String, TopKStringPatterns>> ret = Lists.newArrayList();
    for (FileStatus fileStatus : outputFiles) {
      ret.addAll(FPGrowth.readFrequentPattern(conf, fileStatus.getPath()));
    }
    return ret;
  }
  
  /**
   * 
   * @param params
   *          params should contain input and output locations as a string value, the additional
   *          parameters
   *          include minSupport(3), maxHeapSize(50), numGroups(1000)
   * @throws NoSuchAlgorithmException
   * @throws ParseException
   */
  public static void runPFPGrowth(Parameters params) throws IOException,
      InterruptedException,
      ClassNotFoundException, NoSuchAlgorithmException, ParseException {
    Configuration conf = new Configuration();
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    
    long startTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(endTime - startTime)));
    long stepSize = Long
        .parseLong(params.get(PFPGrowth.PARAM_STEP_SIZE, Long.toString(windowSize)));
    endTime = Math.min(endTime, startTime + windowSize);
    
    int minSupport = Integer.valueOf(params.get(MIN_SUPPORT, "3"));
    String countIn = params.get(COUNT_IN);
    if (countIn == null) {
      countIn = params.get(OUTROOT); // PUT);
    }
    int minFr = params.getInt(MIN_FREQ, MIN_FREQ_DEFAULT);
    int prunePct = params.getInt(PRUNE_PCTILE, PRUNE_PCTILE_DEFAULT);
    
    if (params.get(COUNT_IN) == null) {
      startParallelCounting(params, conf);
    }
    
    if (params.get(GROUP_FIS_IN) == null) {
      // save feature list to dcache
      // List<Pair<String, Long>> fList = readFList(params);
      // saveFList(fList, params, conf);
      
      int fListSize = cacheFList(params, conf, countIn, minSupport, minFr, prunePct);
      
      if (runMode.equals(RunningMode.BlockUpdate)) {
        fListSize = -1;
        Path timeRoot = new Path(countIn).getParent().getParent();
        FileSystem fs = FileSystem.getLocal(conf);
        final long currStartTime = startTime;
        for (FileStatus earlierWindow : fs.listStatus(timeRoot, new PathFilter() {
          @Override
          public boolean accept(Path p) {
            // should have used end time, but it doesn't make a difference,
            // AS LONG AS windows don't overlap
            return Long.parseLong(p.getName()) < currStartTime;
          }
        })) {
          // TODO: At such low frequency and support, does pruning out items with less frequency
          // than minFreq cause loosing itemsets that are frequent but through a longer time frame
          cacheFList(params, conf,
              fs.listStatus(earlierWindow.getPath())[0].getPath().toString(),
              minSupport, minFr, prunePct);
        }
      } else {
        // set param to control group size in MR jobs
        int numGroups = params.getInt(PFPGrowth.NUM_GROUPS,
            PFPGrowth.NUM_GROUPS_DEFAULT);
        int maxPerGroup = fListSize / numGroups;
        if (fListSize % numGroups != 0)
          maxPerGroup++;
        params.set(MAX_PER_GROUP, Integer.toString(maxPerGroup));
      }
      // fList = null;
      
      startParallelFPGrowth(params, conf);
    } else {
      cacheFList(params, conf, countIn, minSupport, minFr, prunePct);
    }
    startAggregating(params, conf);
    
    if (runMode.equals(RunningMode.BlockUpdate)) {
      String indexDirStr;// = params.get(INDEX_OUT);
      // if (indexDirStr == null || indexDirStr.isEmpty()) {
      indexDirStr = FilenameUtils.concat(params.get(OUTPUT), "index");
      // } else {
      // indexDirStr = FilenameUtils.concat(indexDirStr, startTime);
      // indexDirStr = FilenameUtils.concat(indexDirStr, endTime);
      // }
      File indexDir = FileUtils.toFile(new URL(indexDirStr));
      
      // clean up
      FileUtils.deleteQuietly(indexDir);
      
      Path seqPath = new Path(params.get(OUTPUT), FREQUENT_PATTERNS);
      Directory earlierIndex = null;
      
      Path timeRoot = new Path(params.get(OUTPUT)).getParent().getParent();
      FileSystem fs = FileSystem.getLocal(conf);
      
      long mostRecent = Long.MIN_VALUE;
      Path mostRecentPath = null;
      for (FileStatus earlierWindow : fs.listStatus(timeRoot)) {
        long earlierStart = Long.parseLong(earlierWindow.getPath().getName());
        // should have used end time, but it doesn't make a difference,
        // AS LONG AS windows don't overlap
        if (earlierStart < startTime && earlierStart > mostRecent) {
          mostRecentPath = earlierWindow.getPath();
          mostRecent = earlierStart;
        }
      }
      if (mostRecentPath != null) {
        mostRecentPath = fs.listStatus(mostRecentPath)[0].getPath();
        mostRecentPath = new Path(mostRecentPath, "index");
        // earlierIndex = new Directory[1];
        // FIXME: as with anything that involves lucene.. won't work except on a local machine
        earlierIndex = new MMapDirectory(FileUtils.toFile(mostRecentPath.toUri().toURL()));
      }
    }
    // FIXME: When we want to stream, we have to build the index of earlier window
    // ItemSetIndexBuilder.buildIndex(seqPath, indexDir,
    // startTime, Math.min(endTime, startTime + windowSize), earlierIndex);
  }
  
  /**
   * Run the aggregation Job to aggregate the different TopK patterns and group each Pattern by the
   * features
   * present in it and thus calculate the final Top K frequent Patterns for each feature
   */
  public static void startAggregating(Parameters params, Configuration conf)
      throws IOException, InterruptedException, ClassNotFoundException {
    
    conf.set(PFP_PARAMETERS, params.toString());
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    // YA
    // if(Boolean.parseBoolean(params.get(PFPGrowth.PSEUDO, "false"))){
    // conf.set("mapred.tasktracker.map.tasks.maximum", "6");
    // conf.set("mapred.map.child.java.opts", "-Xmx1000M");
    // conf.set("mapred.tasktracker.reduce.tasks.maximum", "6");
    // conf.set("mapred.reduce.child.java.opts", "-Xmx1000M");
    // }
    conf.setInt("mapred.max.map.failures.percent", 10);
    conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit -XX:+HeapDumpOnOutOfMemoryError");
    // END YA
    
    String gfisIn = params.get(PFPGrowth.GROUP_FIS_IN, params.get(OUTPUT));
    
    Path input = new Path(gfisIn, FPGROWTH);
    Job job = new Job(conf, "PFP Aggregator Driver running over input: " + input);
    job.setJarByClass(PFPGrowth.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TopKStringPatterns.class);
    
    FileInputFormat.addInputPath(job, input);
    Path outPath = new Path(params.get(OUTPUT), FREQUENT_PATTERNS);
    
    FileOutputFormat.setOutputPath(job, outPath);
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(AggregatorMapper.class);
    job.setCombinerClass(AggregatorReducer.class);
    job.setReducerClass(AggregatorReducer.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    // HadoopUtil.delete(conf, outPath);
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new IllegalStateException("Job failed!");
    }
  }
  
  /**
   * Count the frequencies of various features in parallel using Map/Reduce
   */
  public static void startParallelCounting(Parameters params, Configuration conf)
      throws IOException, InterruptedException, ClassNotFoundException {
    conf.set(PFP_PARAMETERS, params.toString());
    
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    
    // if(Boolean.parseBoolean(params.get(PFPGrowth.PSEUDO, "false"))){
    // conf.set("mapred.tasktracker.map.tasks.maximum", "3");
    // conf.set("mapred.tasktracker.reduce.tasks.maximum", "3");
    // conf.set("mapred.map.child.java.opts", "-Xmx777M");
    // conf.set("mapred.reduce.child.java.opts", "-Xmx777M");
    // conf.setInt("mapred.max.map.failures.percent", 0);
    // }
    conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit -XX:+HeapDumpOnOutOfMemoryError");
    
    // String input = params.get(INPUT);
    // Job job = new Job(conf, "Parallel Counting Driver running over input: " + input);
    long startTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)); //GMT23JAN2011));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE));
    
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(endTime - startTime)));
    long stepSize = Long
        .parseLong(params.get(PFPGrowth.PARAM_STEP_SIZE, Long.toString(windowSize)));
    
    endTime = Math.min(endTime, startTime + windowSize);
    
    FileSystem fs = FileSystem.get(conf); // TODONE: do I need?getLocal(conf);
    
    Job[] jobArr = new Job[(int) Math.ceil(windowSize / stepSize)];
    for (int j = 0; startTime < endTime; startTime += stepSize, ++j) {
      long jobEnd = startTime + stepSize;
      Job job = new Job(conf, "Parallel counting running over inerval " + startTime + "-" + jobEnd); // endTime);

      // Path outPath = new Path(params.get(OUTPUT), PARALLEL_COUNTING);
      Path outRoot = new Path(params.get(OUTROOT));
      Path stepOutput = new Path(outRoot, startTime + "");
      stepOutput = new Path(stepOutput, jobEnd + "");
      if (fs.exists(stepOutput)) {
        continue;
      }
      jobArr[j] = job;
      Path outPath = new Path(stepOutput, PARALLEL_COUNTING);
      FileOutputFormat.setOutputPath(job, outPath);
      // HadoopUtil.delete(conf, outPath);

      
      job.setJarByClass(PFPGrowth.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);
      
      PartitionByTimestamp.setInputPaths(job, params, conf);
      // FileInputFormat.addInputPath(job, new Path(input));

      // job.setInputFormatClass(HtmlTweetInputFormat.class);
      job.setInputFormatClass(CSVTweetInputFormat.class);
      job.setMapperClass(ParallelCountingMapper.class);
      job.setCombinerClass(ParallelCountingReducer.class);
      job.setReducerClass(ParallelCountingReducer.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      
      job.submit();
      
      // boolean succeeded = job.waitForCompletion(true);
      // if (!succeeded) {
      // throw new IllegalStateException("Job failed!");
      // }
    }
    
    boolean allCompleted;
    do {
      Thread.sleep(1000);
      allCompleted = true;
      for (int j = 0; j < jobArr.length; ++j) {
        if(jobArr[j] == null){
          continue;
        }
        boolean complete = jobArr[j].isComplete();
        allCompleted &= complete;
        if (!complete) {
          String report =
              (j + " (" + jobArr[j].getJobName() + "): map "
                  + StringUtils.formatPercent(jobArr[j].mapProgress(), 0) +
                  " reduce " +
                  StringUtils.formatPercent(jobArr[j].reduceProgress(), 0) + " - Tracking: " + jobArr[j]
                  .getTrackingURL());
          LOG.info(report);
        }
      }
    } while (!allCompleted);
    
    boolean allSuccess = true;
    for (int j = 0; j < jobArr.length; ++j) {
      if(jobArr[j] == null){
        continue;
      }
      boolean success = jobArr[j].isSuccessful();
      allSuccess &= success;
      if (!success) {
        String report =
            (j + " (" + jobArr[j].getJobName() + "): FAILED - Tracking: " + jobArr[j]
                .getTrackingURL());
        LOG.info(report);
      }
    }
    if (!allSuccess) {
      throw new IllegalStateException("Job failed!");
    }
  }
  
  /**
   * Run the Parallel FPGrowth Map/Reduce Job to calculate the Top K features of group dependent
   * shards
   */
  public static void startParallelFPGrowth(Parameters params, Configuration conf)
      throws IOException, InterruptedException, ClassNotFoundException {
    conf.set(PFP_PARAMETERS, params.toString());
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    
    // YA
    // if(Boolean.parseBoolean(params.get(PFPGrowth.PSEUDO, "false"))){
    // conf.set("mapred.tasktracker.map.tasks.maximum", "6");
    // conf.set("mapred.map.child.java.opts", "-Xmx1000M");
    // conf.set("mapred.tasktracker.reduce.tasks.maximum", "6");
    // conf.set("mapred.reduce.child.java.opts", "-Xmx1000M");
    // }
    conf.setInt("mapred.max.map.failures.percent", 10);
    conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit -XX:+HeapDumpOnOutOfMemoryError");
    // END YA
    
    long startTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(endTime - startTime)));
    long stepSize = Long
        .parseLong(params.get(PFPGrowth.PARAM_STEP_SIZE, Long.toString(windowSize)));
    endTime = Math.min(endTime, startTime + windowSize);
    Job job = new Job(conf, "PFPGrowth running over inerval " + startTime + "-" + endTime);
    
    job.setJarByClass(PFPGrowth.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(TransactionTree.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TopKStringPatterns.class);
    
    // FileSystem fs = FileSystem.get(conf); //TODONE: do I need?getLocal(conf);
    PartitionByTimestamp.setInputPaths(job, params, conf);
    // FileInputFormat.addInputPath(job, input);
    
    Path outPath = new Path(params.get(OUTPUT), FPGROWTH);
    FileOutputFormat.setOutputPath(job, outPath);
    
    // HadoopUtil.delete(conf, outPath);
    
    // job.setInputFormatClass(HtmlTweetInputFormat.class);
    job.setInputFormatClass(CSVTweetInputFormat.class);
    if (runMode.equals(RunningMode.BlockUpdate)) {
      job.setMapperClass(ParallelFPStreamMapper.class);
      // job.setCombinerClass(ParallelFPStreamCombiner.class);
      job.setCombinerClass(ParallelFPGrowthCombiner.class);
      job.setReducerClass(ParallelFPStreamReducer.class);
    } else {
      job.setMapperClass(ParallelFPGrowthMapper.class);
      job.setCombinerClass(ParallelFPGrowthCombiner.class);
      job.setReducerClass(ParallelFPGrowthReducer.class);
    }
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new IllegalStateException("Job failed!");
    }
  }
  
  private final Parameters intervalParams;
  
  public PFPGrowth(Parameters pIntervalParams) throws IOException {
    intervalParams = new Parameters(pIntervalParams.toString());
  }
  
  @Override
  public Void call() throws Exception {
    runPFPGrowth(intervalParams);
    return null;
  }
  
}
