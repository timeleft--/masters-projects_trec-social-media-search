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
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.freqtermsets.fpgrowth.FPGrowth;
import org.apache.mahout.math.list.IntArrayList;

import ca.uwaterloo.twitter.ItemSetIndexBuilder;

import com.google.common.collect.Lists;
import com.twitter.corpus.data.CSVTweetInputFormat;
import com.twitter.corpus.data.util.PartitionByTimestamp;

/**
 * 
 * Parallel FP Growth Driver Class. Runs each stage of PFPGrowth as described in the paper
 * http://infolab.stanford.edu/~echang/recsys08-69.pdf
 * 
 */
public final class PFPGrowth implements Callable<Void> {
  
  public static final String ENCODING = "encoding";
  public static final String F_LIST = "fList";
  public static final String G_LIST = "gList";
  public static final String NUM_GROUPS = "numGroups";
  public static final int NUM_GROUPS_DEFAULT = 1000;
  public static final String MAX_PER_GROUP = "maxPerGroup";
  public static final String OUTPUT = "output";
  public static final String MIN_SUPPORT = "minSupport";
  public static final String MAX_HEAPSIZE = "maxHeapSize";
  public static final String INPUT = "input";
  public static final String PFP_PARAMETERS = "pfp.parameters";
  public static final String FILE_PATTERN = "part-*";
  public static final String FPGROWTH = "fpgrowth";
  public static final String FREQUENT_PATTERNS = "frequentpatterns";
  public static final String PARALLEL_COUNTING = "parallelcounting";
  
  public static final String USE_FPG2 = "use_fpg2";
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
  public static final String INDEX_OUT = "index";
  
  public static final String PARAM_WINDOW_SIZE = "windowSize";
  
//  public static final long TREC2011_MIN_TIMESTAMP = 1296130141000L; // 1297209010000L;
    // public static final long GMT23JAN2011 = 1295740800000L;
  
  // Not text input anymore
  // public static final String SPLIT_PATTERN = "splitPattern";
  // public static final Pattern SPLITTER = Pattern.compile("[ ,\t]*[,|\t][ ,\t]*");
  // END YA
  // private PFPGrowth() {
  // }
  
  /**
   * Generates the fList from the serialized string representation
   * 
   * @return Deserialized Feature Frequency List
   */
  public static List<Pair<String, Long>> readFList(Configuration conf) throws IOException {
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
    
    // YA: Lang independent stop words removal
    // FIXME: as below
    Parameters params = new Parameters(conf.get("pfp.parameters", ""));
    int minFr = params.getInt(MIN_FREQ, MIN_FREQ_DEFAULT);
    int prunePct = params.getInt(PRUNE_PCTILE, PRUNE_PCTILE_DEFAULT);
    
    Iterator<Pair<Text, LongWritable>> tempIter = new SequenceFileIterable<Text, LongWritable>(
        fListLocalPath, true, conf).iterator();
    long maxFr = Long.MAX_VALUE;
    if (tempIter.hasNext()) {
      maxFr = tempIter.next().getSecond().get() * prunePct / 100;
    }
    tempIter = null;
    
    for (Pair<Text, LongWritable> record : new SequenceFileIterable<Text, LongWritable>(
        fListLocalPath, true, conf)) {
      String token = record.getFirst().toString();
      char ch0 = token.charAt(0);
      if ((ch0 != '#' && ch0 != '@')
          && (record.getSecond().get() < minFr || record.getSecond().get() > maxFr)) {
        continue;
      }
      list.add(new Pair<String, Long>(token, record.getSecond().get()));
    }
    // END YA
    return list;
  }
  
  /**
   * Serializes the fList and returns the string representation of the List
   * 
   * @return Serialized String representation of List
   */
  public static void saveFList(Iterable<Pair<String, Long>> flist, Parameters params,
      Configuration conf)
      throws IOException {
    Path flistPath = new Path(params.get(OUTPUT), F_LIST);
    FileSystem fs = FileSystem.get(flistPath.toUri(), conf);
    flistPath = fs.makeQualified(flistPath);
    HadoopUtil.delete(conf, flistPath);
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
  
  /**
   * read the feature frequency List which is built at the end of the Parallel counting job
   * 
   * @return Feature Frequency List
   */
  public static List<Pair<String, Long>> readFList(Parameters params) throws IOException {
    int minSupport = Integer.valueOf(params.get(MIN_SUPPORT, "3"));
    Configuration conf = new Configuration();
    
    String countIn = params.get(COUNT_IN);
    if (countIn == null) {
      countIn = params.get(OUTPUT);
    }
    Path parallelCountingPath = new Path(countIn, PARALLEL_COUNTING);
    
    PriorityQueue<Pair<String, Long>> queue = new PriorityQueue<Pair<String, Long>>(11,
        new Comparator<Pair<String, Long>>() {
          @Override
          public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
            int ret = o2.getSecond().compareTo(o1.getSecond());
            if (ret != 0) {
              return ret;
            }
            return o1.getFirst().compareTo(o2.getFirst());
          }
        });
    
    // YA: language indipendent stop words.. the 5% most frequent
    // FIXME: this will remove words from only the mostly used lang
    // i.e. cannot be used for a multilingual task
    int minFr = params.getInt(MIN_FREQ, MIN_FREQ_DEFAULT);
    int prunePct = params.getInt(PRUNE_PCTILE, PRUNE_PCTILE_DEFAULT);
    Path path = new Path(parallelCountingPath, FILE_PATTERN);
    // if(!FileSystem.get(path.toUri(),conf).exists(path)){
    // throw new IOException("Cannot find flist file: " + path);
    // }
    
    Iterator<Pair<Text, LongWritable>> tempIter = new SequenceFileDirIterable<Text, LongWritable>(
        path,
        PathType.GLOB, null, null, true, conf).iterator();
    long maxFreq = Long.MAX_VALUE;
    if (tempIter.hasNext()) {
      maxFreq = tempIter.next().getSecond().get() * prunePct / 100;
    }
    tempIter = null;
    
    for (Pair<Text, LongWritable> record : new SequenceFileDirIterable<Text, LongWritable>(
        path, PathType.GLOB, null, null, true, conf)) {
      String token = record.getFirst().toString();
      char ch0 = token.charAt(0);
      long value = record.getSecond().get();
      if ((ch0 == '#' || ch0 == '@') || (value >= minFr /* Support */&& value <= maxFreq)) {
        queue.add(new Pair<String, Long>(token, value));
      }
    }
    
    List<Pair<String, Long>> fList = Lists.newArrayList();
    while (!queue.isEmpty()) {
      fList.add(queue.poll());
    }
    return fList; // This prevents a null exception when using FP2 --> .subList(0, fList.size());
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
   */
  public static void runPFPGrowth(Parameters params) throws IOException,
      InterruptedException,
      ClassNotFoundException, NoSuchAlgorithmException {
    Configuration conf = new Configuration();
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    
    if (params.get(COUNT_IN) == null) {
      startParallelCounting(params, conf);
    }
    
    if (params.get(GROUP_FIS_IN) == null) {
      // save feature list to dcache
      List<Pair<String, Long>> fList = readFList(params);
      saveFList(fList, params, conf);
      
      // set param to control group size in MR jobs
      int numGroups = params.getInt(PFPGrowth.NUM_GROUPS,
          PFPGrowth.NUM_GROUPS_DEFAULT);
      int maxPerGroup = fList.size() / numGroups;
      if (fList.size() % numGroups != 0)
        maxPerGroup++;
      params.set(MAX_PER_GROUP, Integer.toString(maxPerGroup));
      fList = null;
      
      startParallelFPGrowth(params, conf);
    }
    startAggregating(params, conf);
    
    String startTime = params.get(PFPGrowth.PARAM_INTERVAL_START);
    //        Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)); //GMT23JAN2011));
    String endTime = params.get(PFPGrowth.PARAM_INTERVAL_END);
//        Long.toString(Long.MAX_VALUE));
    
    String indexDirStr = params.get(INDEX_OUT);
    if (indexDirStr == null || indexDirStr.isEmpty()) {
      indexDirStr = FilenameUtils.concat(params.get(OUTPUT), "index");
    } else {
      indexDirStr = FilenameUtils.concat(indexDirStr, startTime);
      indexDirStr = FilenameUtils.concat(indexDirStr, endTime);
    }
    File indexDir = new File(indexDirStr);
    
    // clean up
    FileUtils.deleteQuietly(indexDir);
    
    Path seqPath = new Path(params.get(OUTPUT), FREQUENT_PATTERNS);
    
    ItemSetIndexBuilder.buildIndex(seqPath, indexDir);
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
    
    HadoopUtil.delete(conf, outPath);
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
    
//    String input = params.get(INPUT);
//    Job job = new Job(conf, "Parallel Counting Driver running over input: " + input);
    String startTime = params.get(PFPGrowth.PARAM_INTERVAL_START);
//        Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)); //GMT23JAN2011));
    String endTime = params.get(PFPGrowth.PARAM_INTERVAL_END);
//        Long.toString(Long.MAX_VALUE));
    Job job = new Job(conf, "PFP Growth Driver running over inerval " + startTime + "-" + endTime);
    
    job.setJarByClass(PFPGrowth.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    FileSystem fs = FileSystem.get(conf);
    PartitionByTimestamp.setInputPaths(job, params, fs);
//    FileInputFormat.addInputPath(job, new Path(input));
    
    Path outPath = new Path(params.get(OUTPUT), PARALLEL_COUNTING);
    FileOutputFormat.setOutputPath(job, outPath);
    
    HadoopUtil.delete(conf, outPath);
    
//    job.setInputFormatClass(HtmlTweetInputFormat.class);
    job.setInputFormatClass(CSVTweetInputFormat.class);
    job.setMapperClass(ParallelCountingMapper.class);
    job.setCombinerClass(ParallelCountingReducer.class);
    job.setReducerClass(ParallelCountingReducer.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
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
    // END YA
    
    // Path input = new Path(params.get(INPUT));
    // Job job = new Job(conf, "PFP Growth Driver running over inputs" + Arrays.toString(input));
    String startTime = params.get(PFPGrowth.PARAM_INTERVAL_START);
//        Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)); //GMT23JAN2011));
    String endTime = params.get(PFPGrowth.PARAM_INTERVAL_END);
//        Long.toString(Long.MAX_VALUE));
    Job job = new Job(conf, "PFP Growth Driver running over inerval " + startTime + "-" + endTime);
    
    job.setJarByClass(PFPGrowth.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(TransactionTree.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TopKStringPatterns.class);
    
    FileSystem fs = FileSystem.get(conf);
    PartitionByTimestamp.setInputPaths(job, params, fs);
    // FileInputFormat.addInputPath(job, input);
    
    Path outPath = new Path(params.get(OUTPUT), FPGROWTH);
    FileOutputFormat.setOutputPath(job, outPath);
    
    HadoopUtil.delete(conf, outPath);
    
//    job.setInputFormatClass(HtmlTweetInputFormat.class);
    job.setInputFormatClass(CSVTweetInputFormat.class);
    job.setMapperClass(ParallelFPGrowthMapper.class);
    job.setCombinerClass(ParallelFPGrowthCombiner.class);
    job.setReducerClass(ParallelFPGrowthReducer.class);
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
