package com.twitter.corpus.data.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.twitter.corpus.data.CSVTweetRecordReader;

import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class CalculateStats {
  public static final Pattern SPACE_SPLIT = Pattern.compile("\\s");
  public static void main(String[] args) throws IOException, InterruptedException {
    File[] inFiles = FileUtils.listFiles(new File(args[0]), null, true)
        .toArray(new File[0]);
    File outFile = new File(args[1]);
    FileUtils.openOutputStream(outFile).close();
    
    Path[] inPaths = new Path[inFiles.length];
    long[] offsets  = new long[inFiles.length];
    for (int f = 0; f < inFiles.length; ++f) {
      inPaths[f] = new Path(inFiles[f].toURI().toURL().toString());
      offsets[f] = -1;
    }
    
    CSVTweetRecordReader csvReader = new CSVTweetRecordReader();
    CombineFileSplit inputSplit = new CombineFileSplit(inPaths, offsets);
    Configuration conf = new  Configuration();
    csvReader.initialize(inputSplit, conf);
    
    SummaryStatistics lengthStats = new SummaryStatistics();
    while(csvReader.nextKeyValue()){
      PairOfLongs key = csvReader.getCurrentKey();
      long id = key.getLeftElement();
      long timestamp = key.getRightElement();
      
      PairOfStrings value = csvReader.getCurrentValue();
      String screenName = value.getLeftElement();
      String tweet = value.getRightElement();
      
      String[] tokens = SPACE_SPLIT.split(tweet);
      
      lengthStats.addValue(tokens.length);
    }
    
    FileUtils.write(outFile, lengthStats.toString());
  }
}
