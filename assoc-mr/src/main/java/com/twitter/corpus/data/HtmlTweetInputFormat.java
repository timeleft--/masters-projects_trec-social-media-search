package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import edu.umd.cloud9.io.pair.PairOfStringLong;

public class HtmlTweetInputFormat extends SequenceFileInputFormat<PairOfStringLong, Text>  {
  
  @Override
  public RecordReader<PairOfStringLong, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new HtmlTweetRecordReader();
  }
  
}
