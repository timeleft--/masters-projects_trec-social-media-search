package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import edu.umd.cloud9.io.pair.PairOfStringLong;

public class CSVTweetInputFormat extends CombineFileInputFormat<PairOfStringLong, Text>  {
  
  @Override
  public RecordReader<PairOfStringLong, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new CSVTweetRecordReader();
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  
}
