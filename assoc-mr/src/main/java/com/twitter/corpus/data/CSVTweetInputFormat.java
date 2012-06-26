package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class CSVTweetInputFormat extends CombineFileInputFormat<PairOfLongs, PairOfStrings>  {
  
  @Override
  public RecordReader<PairOfLongs, PairOfStrings> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new CSVTweetRecordReader();
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  
}
