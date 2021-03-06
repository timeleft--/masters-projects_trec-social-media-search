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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.PFPGrowth.RunningMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.TokenIterator;

public final class FPGrowthDriver extends AbstractJob {
  
  private static final Logger log = LoggerFactory.getLogger(FPGrowthDriver.class);
  private static final String DEFAULT_NUM_THREADS = "1";
  private static final String PARAM_NUM_THREADS = "nJobs";
  
  private FPGrowthDriver() {
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new FPGrowthDriver(), args);
  }
  
  /**
   * Run TopK FPGrowth given the input file,
   */
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    
    addOption("minSupport",
        "s",
        "(Optional) The minimum number of times a co-occurrence must be present."
            + " Default Value: 3",
        "3");
    addOption("maxHeapSize",
        "k",
        "(Optional) Maximum Heap Size k, to denote the requirement to mine top K items."
            + " Default value: 50",
        "50");
    addOption(PFPGrowth.NUM_GROUPS,
        "g",
        "(Optional) Number of groups the features should be divided in the map-reduce version."
            + " Doesn't work in sequential version Default Value:" + PFPGrowth.NUM_GROUPS_DEFAULT,
        Integer.toString(PFPGrowth.NUM_GROUPS_DEFAULT));
    // addOption("splitterPattern", "regex",
    // "Regular Expression pattern used to split given string transaction into"
    // + " itemsets. Default value splits comma separated itemsets.  Default Value:"
    // + " \"[ ,\\t]*[,|\\t][ ,\\t]*\" ", "[ ,\t]*[,|\t][ ,\t]*");
    addOption("numTreeCacheEntries",
        "tc",
        "(Optional) Number of entries in the tree cache to prevent duplicate"
            + " tree building. (Warning) a first level conditional FP-Tree might consume a lot of memory, "
            + "so keep this value small, but big enough to prevent duplicate tree building. "
            + "Default Value:5 Recommended Values: [5-10]",
        "5");
    // addOption("method", "method", "Method of processing: sequential|mapreduce", "mapreduce");
    // //"sequential");
    addOption("encoding", "e", "(Optional) The file encoding.  Default value: UTF-8", "UTF-8");
    // addFlag("useFPG2", "2", "Use an alternate FPG implementation");
    addOption(PFPGrowth.COUNT_IN,
        "cnt",
        "(Optional) In case of mapreduce, if this is set parallel counting will be skipped and counts will be read from the path specified");
    // addFlag(PFPGrowth.PSEUDO, "ps",
    // "Running on a Pseudo-Cluster (one machine). Uses hardcoded configurations for each job.");
    addOption(PFPGrowth.GROUP_FIS_IN,
        "gfis",
        "(Optional) In case of mapreduce, if this is set execution will start from the aggregation phase, and group dependent frequent itemsets will be read from the path specified");
    addFlag(AggregatorReducer.MUTUAL_INFO_FLAG,
        "mi",
        "Set to selec the top K patterns based on the Normalized Mutual Information rather than frequency of pattern");
    addOption(ParallelFPGrowthReducer.MIN_WORDS_FOR_LANG_ID,
        "lid",
        "The mimun length of a pattern that would be used for language identification");
    addOption(PFPGrowth.MIN_FREQ,
        "mf",
        "The minimum frequency of a token. Any token with less frequency will be pruned from the begining.");
    addOption(PFPGrowth.PRUNE_PCTILE,
        "pct",
        "The percentile of frequencies that will be considered; any token with a higher frequency will be pruned");
//    addFlag("shift", "shift", "If set (and window must be set) it shifts the window by half");
    addFlag(TokenIterator.PARAM_REPEAT_HASHTAG,
        "rht",
        "If set, each hashtag is repeated, removing the # sign from the second token returned for the same hashtag");
    addOption(PFPGrowth.PARAM_INTERVAL_START, "st", "The start time of interval to be mined.. defaults to first known tweet time");
    addOption(PFPGrowth.PARAM_INTERVAL_END, "et", "The end time of interval to be mined.. defaults to long.maxvalue");
    addOption(PFPGrowth.PARAM_WINDOW_SIZE, "ws", "The duration of windows that will be mined.. defaults to end - start");
    addOption(PFPGrowth.PARAM_STEP_SIZE, "ss", "The step by which the window will be advanced.. defaults to windowSize");
    
    addOption(PARAM_NUM_THREADS,
        "j",
        "The number of PFP jobs, because in case of intervals resources are under utilized");
    
    // addOption(PFPGrowth.INDEX_OUT,
    // "ix",
    // "The local folder to which the frequent itemset index will be written");
    
    if (parseArguments(args) == null) {
      return -1;
    }
    
    Parameters params = new Parameters();
    
    if (hasOption("minSupport")) {
      String minSupportString = getOption("minSupport");
      params.set("minSupport", minSupportString);
    }
    if (hasOption("maxHeapSize")) {
      String maxHeapSizeString = getOption("maxHeapSize");
      params.set("maxHeapSize", maxHeapSizeString);
    }
    if (hasOption(PFPGrowth.NUM_GROUPS)) {
      String numGroupsString = getOption(PFPGrowth.NUM_GROUPS);
      params.set(PFPGrowth.NUM_GROUPS, numGroupsString);
    }
    
    if (hasOption("numTreeCacheEntries")) {
      String numTreeCacheString = getOption("numTreeCacheEntries");
      params.set("treeCacheSize", numTreeCacheString);
    }
    
    // if (hasOption("splitterPattern")) {
    // String patternString = getOption("splitterPattern");
    // params.set("splitPattern", patternString);
    // }
    
    String encoding = "UTF-8";
    if (hasOption("encoding")) {
      encoding = getOption("encoding");
    }
    params.set("encoding", encoding);
    
    // if (hasOption("useFPG2")) {
    // params.set(PFPGrowth.USE_FPG2, "true");
    // }
    
    // if (hasOption(PFPGrowth.COUNT_IN)) {
    // params.set(PFPGrowth.COUNT_IN, getOption(PFPGrowth.COUNT_IN));
    // }
    
    // if(hasOption(PFPGrowth.PSEUDO)){
    // params.set(PFPGrowth.PSEUDO, "true");
    // }
    
    // if (hasOption(PFPGrowth.GROUP_FIS_IN)) {
    // params.set(PFPGrowth.GROUP_FIS_IN, getOption(PFPGrowth.GROUP_FIS_IN));
    // }
    
    if (hasOption(AggregatorReducer.MUTUAL_INFO_FLAG)) {
      params.set(AggregatorReducer.MUTUAL_INFO_FLAG, "true");
    } else {
      params.set(AggregatorReducer.MUTUAL_INFO_FLAG, "false");
    }
    
    if (hasOption(ParallelFPGrowthReducer.MIN_WORDS_FOR_LANG_ID)) {
      params.set(ParallelFPGrowthReducer.MIN_WORDS_FOR_LANG_ID,
          getOption(ParallelFPGrowthReducer.MIN_WORDS_FOR_LANG_ID));
    }
    
    if (hasOption(PFPGrowth.MIN_FREQ)) {
      params.set(PFPGrowth.MIN_FREQ, getOption(PFPGrowth.MIN_FREQ));
    }
    
    if (hasOption(PFPGrowth.PRUNE_PCTILE)) {
      params.set(PFPGrowth.PRUNE_PCTILE, getOption(PFPGrowth.PRUNE_PCTILE));
    }
    
    // if (hasOption(PFPGrowth.PARAM_INTERVAL_END)) {
    params.set(PFPGrowth.PARAM_INTERVAL_END,
        getOption(PFPGrowth.PARAM_INTERVAL_END, Long.toString(Long.MAX_VALUE)));
    // }
    
    if (hasOption(PFPGrowth.PARAM_WINDOW_SIZE)) {
      params.set(PFPGrowth.PARAM_WINDOW_SIZE, getOption(PFPGrowth.PARAM_WINDOW_SIZE));
    }
    
    if (hasOption(PFPGrowth.PARAM_STEP_SIZE)) {
      params.set(PFPGrowth.PARAM_STEP_SIZE, getOption(PFPGrowth.PARAM_STEP_SIZE));
    }
    
    // if (hasOption(PFPGrowth.PARAM_INTERVAL_START)) {
    // params.set(PFPGrowth.PARAM_INTERVAL_START, getOption(PFPGrowth.PARAM_INTERVAL_START));
    // }
    
    // if (hasOption(PFPGrowth.INDEX_OUT)) {
    // params.set(PFPGrowth.INDEX_OUT, getOption(PFPGrowth.INDEX_OUT));
    // }
    
    if (hasOption(TokenIterator.PARAM_REPEAT_HASHTAG)) {
      params.set(TokenIterator.PARAM_REPEAT_HASHTAG, "true");
    }
    
//    boolean shiftedWindow = hasOption("shift");
    
    Path inputDir = getInputPath();
    Path outputDir = getOutputPath();
    
    params.set(PFPGrowth.INPUT, inputDir.toString());
    params.set(PFPGrowth.OUTROOT, outputDir.toString());
    
    Configuration conf = new Configuration();
//    HadoopUtil.delete(conf, outputDir);
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(outputDir)){
      throw new IllegalArgumentException("Output path already exists.. please delete it yourself: " + outputDir);
    }
    
    int nThreads = Integer.parseInt(getOption(PARAM_NUM_THREADS, DEFAULT_NUM_THREADS));
    if (!PFPGrowth.runMode.equals(RunningMode.Batch) && nThreads != 1) {
      throw new UnsupportedOperationException("We use mining results from earlier windows. j must be 1");
    }
    ExecutorService exec = Executors.newFixedThreadPool(nThreads);
    Future<Void> lastFuture = null;
    
    String startTimeStr = getOption(PFPGrowth.PARAM_INTERVAL_START);
    // params.get(PFPGrowth.PARAM_INTERVAL_START);
    if (startTimeStr == null) {
      // FIXME: Will fail if not running locally.. like many things now
      // FileSystem fs = FileSystem.getLocal(conf);
      // startTimeStr = fs.listStatus(inputDir)[0].getPath().getName();
      File[] startFolders = FileUtils.toFile(inputDir.toUri().toURL()).listFiles();
      Arrays.sort(startFolders);
      startTimeStr = startFolders[0].getName();
    }
    long startTime = Long.parseLong(startTimeStr);
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)));// GMT23JAN2011)));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE)));
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(endTime - startTime)));
    long stepSize = Long
        .parseLong(params.get(PFPGrowth.PARAM_STEP_SIZE, Long.toString(windowSize)));
    
    // int numJobs = 0;
    while (startTime < endTime) {
      // if(++numJobs % 100 == 0){
      // Thread.sleep(60000);
      // }
      long shift = 0;
//      if(shiftedWindow){
//        shift = (long)Math.floor(windowSize / 2.0f);
//      }
      params.set(PFPGrowth.PARAM_INTERVAL_START, Long.toString(startTime + shift));
      
      if (hasOption(PFPGrowth.GROUP_FIS_IN)) {
        String gfisIn = getOption(PFPGrowth.GROUP_FIS_IN);
        gfisIn = FilenameUtils.concat(gfisIn, Long.toString(startTime + shift));
        gfisIn = FilenameUtils.concat(gfisIn,
            Long.toString(Math.min(endTime, startTime + windowSize) + shift));
        params.set(PFPGrowth.GROUP_FIS_IN, gfisIn);
      }
      
      if (hasOption(PFPGrowth.COUNT_IN)) {
        String countIn = getOption(PFPGrowth.COUNT_IN);
//        countIn = FilenameUtils.concat(countIn, Long.toString(startTime + shift));
//        countIn = FilenameUtils.concat(countIn,
//            Long.toString(Math.min(endTime, startTime + windowSize) + shift));
        params.set(PFPGrowth.COUNT_IN, countIn);
      }
      
      String outPathStr = FilenameUtils.concat(outputDir.toString(), Long.toString(startTime + shift));
      outPathStr = FilenameUtils.concat(outPathStr,
          Long.toString(Math.min(endTime, startTime + windowSize) + shift));
      params.set(PFPGrowth.OUTPUT, outPathStr);
      
      // PFPGrowth.runPFPGrowth(params);
      lastFuture = exec.submit(new PFPGrowth(params));
      
//      startTime += windowSize;
      startTime += stepSize;
      
//      Thread.sleep(10000);
    }
    
    lastFuture.get();
    exec.shutdown();
    
    while (!exec.isTerminated()) {
      Thread.sleep(1000);
    }
    
    return 0;
  }
  
  private static void runFPGrowth(Parameters params) throws IOException {
    throw new UnsupportedOperationException();
    // log.info("Starting Sequential FPGrowth");
    // int maxHeapSize = Integer.valueOf(params.get("maxHeapSize", "50"));
    // int minSupport = Integer.valueOf(params.get("minSupport", "3"));
    //
    // String output = params.get("output", "output.txt");
    //
    // Path path = new Path(output);
    // Configuration conf = new Configuration();
    // FileSystem fs = FileSystem.get(path.toUri(), conf);
    //
    // Charset encoding = Charset.forName(params.get("encoding"));
    // String input = params.get("input");
    //
    // String pattern = params.get("splitPattern", PFPGrowth.SPLITTER.toString());
    //
    // SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class,
    // TopKStringPatterns.class);
    //
    // if ("true".equals(params.get("useFPG2"))) {
    // org.apache.mahout.freqtermsets.fpgrowth2.FPGrowthObj<String> fp
    // = new org.apache.mahout.freqtermsets.fpgrowth2.FPGrowthObj<String>();
    // Collection<String> features = new HashSet<String>();
    //
    // try {
    // fp.generateTopKFrequentPatterns(
    // new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
    // fp.generateFList(
    // new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
    // minSupport),
    // minSupport,
    // maxHeapSize,
    // features,
    // new StringOutputConverter(new SequenceFileOutputCollector<Text, TopKStringPatterns>(writer)),
    // new ContextStatusUpdater(null));
    // } finally {
    // Closeables.closeQuietly(writer);
    // }
    // } else {
    // FPGrowth<String> fp = new FPGrowth<String>();
    // Collection<String> features = new HashSet<String>();
    // try {
    // fp.generateTopKFrequentPatterns(
    // new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
    // fp.generateFList(
    // new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
    // minSupport),
    // minSupport,
    // maxHeapSize,
    // features,
    // new StringOutputConverter(new SequenceFileOutputCollector<Text, TopKStringPatterns>(writer)),
    // new ContextStatusUpdater(null));
    // } finally {
    // Closeables.closeQuietly(writer);
    // }
    // }
    //
    //
    // List<Pair<String, TopKStringPatterns>> frequentPatterns = FPGrowth.readFrequentPattern(conf,
    // path);
    // for (Pair<String, TopKStringPatterns> entry : frequentPatterns) {
    // log.info("Dumping Patterns for Feature: {} \n{}", entry.getFirst(), entry.getSecond());
    // }
  }
}
