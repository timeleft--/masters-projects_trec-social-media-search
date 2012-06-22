package com.twitter.corpus.data;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.pair.PairOfStringLong;

public class CSVTweetRecordReader extends RecordReader<PairOfStringLong, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(CSVTweetRecordReader.class);
  private static final Pattern tabSplit = Pattern.compile("\\t");
  
  private FSDataInputStream reader;
  private TaskAttemptContext context;
  private PairOfStringLong myKey;
  private Text myValue;
  
  private Configuration conf;
  
  private int currFile = 0;
  
  private FileSystem fs;
  private CombineFileSplit split;
  
  @SuppressWarnings("deprecation")
  private boolean openNextFile() throws IOException {
    if (reader != null) {
      reader.close();
    }
    if (currFile >= split.getNumPaths()) {
      return false;
    }
    reader = fs.open(split.getPath(currFile++));
    reader.readLine();
    return true;
  }
  
  @SuppressWarnings("deprecation")
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    myKey = null;
    myValue = null;
    String line;
    while ((line = reader.readLine()) == null) {
      if (!openNextFile()) {
        return false;
      }
    }
    
    String[] fields = tabSplit.split(line);
    if (fields.length < 4) {
      return nextKeyValue();
    }
    
    String tweet = StringEscapeUtils.unescapeJava(fields[3]);
    
    String screenName = fields[1];
    long id = Long.parseLong(fields[0]);
    long timestamp = Long.parseLong(fields[2]);
    
    myKey = new PairOfStringLong(screenName, timestamp);
    
    myValue = new Text(/* "@" + screenName + ": " + */tweet);
    return true;
  }
  
  @Override
  public void initialize(InputSplit pSplit, TaskAttemptContext pContext) throws IOException,
      InterruptedException {
    split = ((CombineFileSplit) pSplit);
    context = pContext;
    conf = context.getConfiguration();
    fs = FileSystem.get(conf);
    openNextFile();
  }
  
  @Override
  public PairOfStringLong getCurrentKey() throws IOException, InterruptedException {
    return myKey;
  }
  
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return myValue;
  }
  
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 1.0f * currFile / split.getNumPaths();
  }
  
  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }
}
