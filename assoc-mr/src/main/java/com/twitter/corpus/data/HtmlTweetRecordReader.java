package com.twitter.corpus.data;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.pair.PairOfLongString;
import edu.umd.cloud9.io.pair.PairOfStringLong;

public class HtmlTweetRecordReader extends RecordReader<PairOfStringLong, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(HtmlTweetRecordReader.class);
  
  SequenceFileRecordReader<PairOfLongString, HtmlStatus> reader;
  PairOfStringLong myKey;
  Text myValue;
  
  public HtmlTweetRecordReader() {
    reader = new SequenceFileRecordReader<PairOfLongString, HtmlStatus>();
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    
    if (reader.nextKeyValue()) {
      HtmlStatus origValue = reader.getCurrentValue();
      int httpStatus = origValue.getHttpStatusCode();
      
      // Note that http status code 302 indicates a redirect, which indicates a retweet. I.e.,
      // the status redirects to the original retweeted status. Note that this is not currently
      // handled. We neglect retweets. 301 indicates a changed username, but content is null
      if (httpStatus >= 300) {
        return nextKeyValue();
      } else {
        String html = origValue.getHtml();
        if (html == null || html.isEmpty()) {
          return nextKeyValue();
        }
        
        String tweet = extractTweet(html);
        if (tweet == null || tweet.isEmpty()) {
          return nextKeyValue();
        }
        
        PairOfLongString origKey = reader.getCurrentKey();
        String screenName = origKey.getRightElement();
        long id = origKey.getLeftElement();
        Long timestamp = extractTimestamp(html, id);
        
        myKey = new PairOfStringLong(screenName, timestamp);
        
        myValue = new Text(/* "@" + screenName + ": " + */tweet);
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
  public PairOfStringLong getCurrentKey() throws IOException, InterruptedException {
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
  
  private final Pattern TIMESTAMP_PATTERN =
      Pattern.compile("<span class=\"published timestamp\" data=\"\\{time:'([^']+)'\\}\">");
  
  // Sun Jan 23 00:00:00 +0000 2011
  private final DateFormat dFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
  
  public Long extractTimestamp(String html, long id) {
    Preconditions.checkNotNull(html);
    Matcher matcher = TIMESTAMP_PATTERN.matcher(html);
    Long result = null;
    
    // // most tweets have a single timestamp in the HTML
    // // retweets ONLY have the retweet timestamp in the HTML
    // // replies have both the reply tweet and the original tweet's timestamps in
    // // the HTML, so we have to check the timestamp id
    // String timestamp = null;
    // while (matcher.find()) {
    // timestamp = matcher.group(3);
    // long id_i = Long.parseLong(matcher.group(2));
    // if (id_i == id) {
    // try {
    // result = dFmt.parse(timestamp).getTime();
    // } catch (ParseException e) {
    // LOG.error(e.getMessage(), e);
    // return null;
    // }
    // }
    
    if (!matcher.find()) {
      result = null;
    }
    try {
      result = dFmt.parse(matcher.group(1)).getTime();
    } catch (ParseException e) {
      LOG.error(e.getMessage(), e);
      result = null;
    }
    return result;
  }
}
