package com.twitter.corpus.data;

import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.pair.PairOfLongString;



public class HtmlTweetRecordReader extends RecordReader<Text, Text>  {
//  private static final Logger LOG = LoggerFactory.getLogger(HtmlTweetRecordReader.class);
  
  SequenceFileRecordReader<PairOfLongString,HtmlStatus> reader;
  Text myKey;
  Text myValue;
  
  public HtmlTweetRecordReader() {
    reader = new SequenceFileRecordReader<PairOfLongString, HtmlStatus>();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
   
    if(reader.nextKeyValue()){
      HtmlStatus origValue = reader.getCurrentValue();
      int httpStatus = origValue.getHttpStatusCode();
      
      // Note that http status code 302 indicates a redirect, which indicates a retweet. I.e.,
      // the status redirects to the original retweeted status. Note that this is not currently
      // handled. We neglect retweets. 301 indicates a changed username, but content is null
      if(httpStatus >= 300){
        return nextKeyValue();
      } else {
        PairOfLongString origKey = reader.getCurrentKey();
        myKey = new Text(origKey.getRightElement() + "/" + origKey.getLeftElement());
        String html = origValue.getHtml();
        if(html ==  null || html.isEmpty()){
          return nextKeyValue();
        }
        String tweet = extractTweet(html);
        if(tweet == null || tweet.isEmpty()){
          return nextKeyValue();
        }
        myValue = new Text(tweet);
        return true;
      }
    }
    myKey = null;
    myValue = null;
    return false;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    reader.initialize(split, context);
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return myKey;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return myValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return reader.getProgress();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
  
  private final String TWEET_BEGIN_DELIMITER = "<span class=\"entry-content\">";
  private final String TWEET_END_DELIMITER = "<span class=\"meta entry-meta\"";

  public String extractTweet(String html) {
    Preconditions.checkNotNull(html);
    int begin = html.indexOf(TWEET_BEGIN_DELIMITER);
    int end = html.indexOf(TWEET_END_DELIMITER);

    if (begin == -1 || end == -1) {
      return null;
    }

    String raw = html.substring(begin, end);
    raw = raw.replaceAll("<(.|\n)*?>", "").replaceAll("[\n\r]", "").replaceAll("[\t]", " ").trim();
    raw = StringEscapeUtils.unescapeHtml(raw);

    return raw;
  }
}
