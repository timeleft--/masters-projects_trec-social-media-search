package com.twitter.corpus.data.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.PFPGrowth;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenLongObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.hadoop.util.CorpusReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.corpus.data.HtmlStatus;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class PartitionByTimestamp {
  private static final Logger LOG = LoggerFactory
      .getLogger(PartitionByTimestamp.class);
  
  public static final long FILE_INTERVAL_MILLIS = 5 * 60 * 1000;
  public static final long FOLDER_INTERVAL_MILLIS = 60 * 60 * 1000;
  private static final int MAX_OPEN_FILES = 333;
  
  private static Map<String, List<Pair<Long, Path>>> chilrdenCache = Collections
      .synchronizedMap(new WeakHashMap<String, List<Pair<Long, Path>>>());
  
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path htmlPath = new Path(
        "file:///u2/yaboulnaga/datasets/twitter-trec2011/html");
    CorpusReader<PairOfLongString, HtmlStatus> reader = new CorpusReader<PairOfLongString, HtmlStatus>(
        htmlPath, fs, ".*\\.html\\.seq");
    
    Path outPath = new Path(
        "file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/html_hour-5min");
    // "hdfs://scspc400.cs.uwaterloo.ca:9000/twitter2011/byhour/");
    
    // HadoopUtil.delete(conf, outPath);
    // FileUtils.deleteQuietly(outPath);
    if (fs.exists(outPath)) {
      throw new IllegalArgumentException("Out Path already exists: "
          + outPath);
    }
    
    OpenIntIntHashMap statusCount = new OpenIntIntHashMap();
    
    Set<String> invalidTweetsSet = Sets.newHashSet();
    Path validTweetsFile = new Path(
        "file:///u2/yaboulnaga/datasets/twitter-trec2011/tweetids/ian_may2012/ian-id-status.01-May-2012");
    BufferedReader validTweetsRd = new BufferedReader(Channels.newReader(
        FileUtils.openInputStream(
            FileUtils.toFile(validTweetsFile.toUri().toURL()))
            .getChannel(), "UTF-8"));
    String line;
    Pattern tabSplit = Pattern.compile("\\s");
    while ((line = validTweetsRd.readLine()) != null) {
      String[] fields = tabSplit.split(line);
      if (!("200".equals(fields[1]) || "301".equals(fields[1]))) {
        invalidTweetsSet.add(fields[0]);
      }
    }
    
    // DefaultStringifier<Pair<Pair<Long, String>, HtmlStatus>> stringifier
    // =
    // new DefaultStringifier<Pair<Pair<Long, String>, HtmlStatus>>(
    // conf, GenericsUtil.getClass(new Pair<Pair<Long, String>, HtmlStatus>(
    // new Pair<Long, String>(null, null), new HtmlStatus())));
    
    OpenLongObjectHashMap<Path> folders = new OpenLongObjectHashMap<Path>();
    LinkedHashMap<Path, Writer> writers = Maps.newLinkedHashMap();
    try {
      Pair<PairOfLongString, HtmlStatus> record;
      while ((record = reader.next()) != null) {
        long id = record.getFirst().getLeftElement();
        HtmlStatus htmlStatus = record.getSecond();
        // // FIXME: 301 when the new corpus arrives
        // if (htmlStatus.getHttpStatusCode() >= 300) {
        // continue;
        // }
        String tweet = extractTweet(htmlStatus.getHtml());
        // if (tweet == null || tweet.isEmpty()) {
        // statusCount.put(0,
        // statusCount.get(0) + 1);
        // continue;
        // }
        // Long timestamp = extractTimestamp(htmlStatus.getHtml(), id);
        // // if (timestamp == null) {
        // // continue;
        // // }
        int htmlResponse = htmlStatus.getHttpStatusCode();
        if ((tweet == null || tweet.isEmpty()) // || (timestamp ==
            // null))
            && (htmlResponse == 200 || htmlResponse == 301)) {
          htmlResponse = 0;
        }
        statusCount
            .put(htmlResponse, statusCount.get(htmlResponse) + 1);
        
        if (invalidTweetsSet.contains("" + id) || htmlResponse == 0
            || !(htmlResponse == 200 || htmlResponse == 301)) {
          continue;
        }
        
        Long timestamp = extractTimestamp(htmlStatus.getHtml(), id);
        if (timestamp == null) {
          statusCount.put(-1, statusCount.get(-1) + 1);
          LOG.debug(
              "Null timestamp for tweet with id {} and http response {}",
              id, htmlResponse);
          continue;
        }
        
        long hourstamp = getHourTimestamp(timestamp);
        
        if (!folders.containsKey(hourstamp)) {
          Path folder = new Path(outPath, "" + hourstamp);
          folders.put(hourstamp, folder);
          long fileend = hourstamp + FILE_INTERVAL_MILLIS;
          long folderend = hourstamp + FOLDER_INTERVAL_MILLIS;
          while (fileend <= folderend) { // <= in creation, because it
            // is not inclusive
            if (writers.size() >= MAX_OPEN_FILES) {
              Iterator<Entry<Path, Writer>> writersIter = writers
                  .entrySet().iterator();
              Writer wr = writersIter.next().getValue();
              wr.flush();
              wr.close();
              writersIter.remove();
            }
            
            Path filePath = new Path(folder, "" + fileend);
            Writer wr = Channels.newWriter(FileUtils
                .openOutputStream(FileUtils.toFile(filePath.toUri().toURL())).getChannel(),
                "UTF-8");
            writers.put(filePath, wr);
            wr.append("id\tscreenname\ttimestamp\ttweet\n");
            // FSDataOutputStream seqout =
            // FileSystem.create(fs, filePath,
            // FsPermission.getDefault());
            // writers.put(filePath, SequenceFile.createWriter(conf,
            // seqout,
            // PairOfLongString.class, HtmlStatus.class,
            // SequenceFile.CompressionType.NONE, null));
            
            fileend += FILE_INTERVAL_MILLIS;
          }
        }
        
        Path hourFolder = folders.get(hourstamp);
        
        Pair<Long, Path> timePath = pathForTime(hourFolder, timestamp,
            fs);
        
        long fileEndTime = timePath.getFirst();
        
        if (!writers.containsKey(timePath.getSecond())) {
          if (writers.size() >= MAX_OPEN_FILES) {
            Iterator<Entry<Path, Writer>> writersIter = writers
                .entrySet().iterator();
            Writer wr = writersIter.next().getValue();
            wr.flush();
            wr.close();
            writersIter.remove();
          }
          
          writers.put(timePath.getSecond(), Channels.newWriter(FileUtils
              .openOutputStream(FileUtils.toFile(timePath.getSecond()
                  .toUri().toURL())).getChannel(), "UTF-8"));
          // FSDataOutputStream seqout =
          // FileSystem.open(timePath.getSecond());
          // writers.put(timePath.getSecond(),
          // SequenceFile.createWriter(fs, conf,
          // timePath.getSecond(),
          // PairOfLongString.class, HtmlStatus.class,
          // SequenceFile.CompressionType.NONE));
          // //BLOCK));
        }
        
        Writer wr = writers.get(timePath.getSecond());
        
        long folderStartTime = Long.parseLong(hourFolder.getName());
        long folderEndTime = folderStartTime + FOLDER_INTERVAL_MILLIS;
        
        assert (timestamp >= folderStartTime
            && timestamp < folderEndTime && timestamp < fileEndTime) : "Timestamp "
            + timestamp
            + " doesn't fit in the prepared folder/file "
            + folderEndTime + "/" + fileEndTime;
        
        // wr.append(record.getFirst(), record.getSecond());
        // wr.append(stringifier.toString(new Pair<Pair<Long, String>,
        // HtmlStatus>(
        // new Pair<Long, String>(record.getFirst().getKey(),
        // record.getFirst().getValue()),
        // record.getSecond())) + DELIMITER);
        wr.append(record.getFirst().getLeftElement() + "\t"
            + record.getFirst().getRightElement() + "\t"
            + timestamp + "\t"
            // I don't know what this is, but it's different from
            // the extracted timestamp
            // record.getSecond().getTimestamp() + "\t"
            + StringEscapeUtils.escapeJava(tweet) + "\n");
      }
      FileUtils.writeStringToFile(
          FileUtils.toFile(new Path(outPath, "httpStatusCount.txt").toUri().toURL()),
          statusCount.toString());
    } finally {
      reader.close();
      
      for (Writer wr : writers.values()) {
        wr.flush();
        wr.close();
      }
    }
  }
  
  private static long getHourTimestamp(Long timestamp) {
    GregorianCalendar calendar = new GregorianCalendar(
        TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(timestamp);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return calendar.getTimeInMillis();
  }
  
  // public static Pair<Long, Path> findForTime(
  // Path parent, long time,
  // // always looking for start when ascending and end when descending
  // boolean startOrEnd, FileSystem fs) throws IOException {
  // // boolean ascending, ArrayList<Character> trust) {
  // int direction = (startOrEnd ? 1 : -1);
  //
  // List<Pair<Long, Path>> timeList = getChildrenTimeList(parent, startOrEnd,
  // fs);
  //
  // int start = 0;
  // int end = timeList.size() - 1;
  // int i;
  // do {
  // // end-start/2 will be zero in case of a difference of +1 or -1
  // i = start + ((end - start) / 2);
  //
  // int comparison = timeList.get(i).getFirst().compareTo(time);
  //
  // comparison *= direction;
  //
  // if (comparison == 0) {
  // break;
  // } else if (comparison < 0) {
  // start = i + 1;
  // } else {
  // end = i - 1;
  // }
  // } while (start <= end);
  //
  // while (i >= 0 && i < timeList.size()) {
  // Pair<Long, Path> result = timeList.get(i);
  // if ((startOrEnd && result.getFirst() <= time)
  // ||(!startOrEnd && result.getFirst() > time)) {
  // return result;
  // }
  // i += direction;
  // }
  //
  // return null;
  // }
  
  // private static Pair<Long, File> fileForTime(File hourFolder, long
  // exactTime, FileSystem fs)
  // throws IOException {
  // Pair<Long, File> result = null;
  // for (Pair<Long, File> timeFile : getChildrenTimeList(hourFolder, false))
  // {
  // if (timeFile.getFirst() > exactTime) {
  // result = timeFile;
  // } else {
  // break;
  // }
  // }
  // return result;
  // }
  
  private static Pair<Long, Path> pathForTime(Path hourFolder,
      long exactTime, FileSystem fs) throws IOException {
    Pair<Long, Path> result = null;
    for (Pair<Long, Path> timeFile : getChildrenTimeList(hourFolder, false, true,
        fs)) {
      if (timeFile.getFirst() > exactTime) {
        result = timeFile;
      } else {
        break;
      }
    }
    return result;
  }
  /**
   * 
   * @param parent
   * @param ascendingOrDescending true ascending, false descending
   * @param filesOrDirs true files, false dirs
   * @param fs
   * @return
   * @throws IOException
   */
  private static List<Pair<Long, Path>> getChildrenTimeList(Path parent,
      boolean ascendingOrDescending, boolean filesOrDirs, FileSystem fs) throws IOException {
    int direction = ascendingOrDescending ? 1 : -1;
    List<Pair<Long, Path>> timeList;
    String cacheKey = parent.toString() + direction;
    if (chilrdenCache.containsKey(cacheKey)) {
      timeList = chilrdenCache.get(cacheKey);
    } else {
      List<Path> children = new LinkedList<Path>();
      for (FileStatus status : fs.listStatus(parent)) {
        if (filesOrDirs ^ status.isDir()) {
          children.add(status.getPath());
        }
      }
      Collections.sort(children);
      // File[] children = FileUtils.listFiles(parent, null,
      // false).toArray(new File[0]); // fs.listStatus(parent);
      // TODO: provide OS indpendent comparator
      // Arrays.sort(children);
      int childrenlength = children.size();
      timeList = new ArrayList<Pair<Long, Path>>(childrenlength);
      
      int i = (ascendingOrDescending ? 0 : childrenlength - 1);
      while (i >= 0 && i < childrenlength) {
        // File child = children[i];
        Path child = children.get(i);
        long childTime = Long.parseLong(child.getName());
        timeList.add(new Pair<Long, Path>(childTime, child));
        i += direction;
      }
      chilrdenCache.put(cacheKey, timeList);
    }
    return timeList;
  }
  
  public static void setInputPaths(Job job, Parameters params, Configuration conf) // , FileSystem
                                                                                   // fs)
      throws IOException {
    
    // File parent = FileUtils.toFile(new URL(params.get(PFPGrowth.INPUT)));
    Path parent = new Path(params.get(PFPGrowth.INPUT));
    
    FileSystem fs = FileSystem.get(parent.toUri(), conf);
    
    long startTime = Long.parseLong(params
        .get(PFPGrowth.PARAM_INTERVAL_START));
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE)));
    long windowSize;
    String wsStr = params.get(PFPGrowth.PARAM_WINDOW_SIZE);
    if (wsStr == null) {
      List<Pair<Long, Path>> startFolders = getChildrenTimeList(parent, false, false, fs);
      windowSize = startFolders.get(0).getFirst() + FOLDER_INTERVAL_MILLIS - startTime;
      // No, we can't default to this here, coz we'll keep iterating on imaginary folders
      // Long.toString(endTime - startTime)));
    } else {
      windowSize = Long.parseLong(wsStr);
    }
    // YA 20121101
    long stepSize = Long
        .parseLong(params.get(PFPGrowth.PARAM_STEP_SIZE, Long.toString(windowSize)));
    endTime = Math.min(endTime, startTime + stepSize - 1);
//    endTime = Math.min(endTime, startTime + windowSize - 1);
    
    long rhs = endTime + FILE_INTERVAL_MILLIS;
    while (startTime < endTime) {
      long hourstamp = getHourTimestamp(startTime);
      Pair<Long, Path> folder = new Pair<Long, Path>(hourstamp, new Path(
          parent, hourstamp + ""));
      
      assert (folder.getFirst() >= startTime && folder.getFirst() < endTime) : "Folder "
          + folder.getSecond()
          + " is not suitable for the interval "
          + startTime + "-" + endTime;
      List<Pair<Long, Path>> children = getChildrenTimeList(
          folder.getSecond(), true, true, fs);
      int i = 0;
      while (i < children.size()) {
        Pair<Long, Path> child = children.get(i);
        if (child.getFirst() < rhs) {
          if (child.getFirst() > startTime) {
            FileInputFormat.addInputPath(job, child.getSecond());
          }
        } else {
          break;
        }
        ++i;
      }
      
      startTime += PartitionByTimestamp.FOLDER_INTERVAL_MILLIS;
    }
  }
  
  private static final String TWEET_BEGIN_DELIMITER = "<span class=\"entry-content\">";
  private static final String TWEET_END_DELIMITER = "<span class=\"meta entry-meta\"";
  
  public static String extractTweet(String html) {
    Preconditions.checkNotNull(html);
    int begin = html.indexOf(TWEET_BEGIN_DELIMITER);
    int end = html.indexOf(TWEET_END_DELIMITER);
    
    if (begin == -1 || end == -1) {
      return null;
    }
    
    String raw = html.substring(begin, end);
    raw = raw.replaceAll("<(.|\n)*?>", "").replaceAll("[\n\r]", "")
        .replaceAll("[\t]", " ").trim();
    raw = StringEscapeUtils.unescapeHtml(raw);
    
    return raw;
  }
  
  private static final Pattern TIMESTAMP_PATTERN = Pattern
      .compile("<span class=\"published timestamp\" data=\"\\{time:'([^']+)'\\}\">");
  
  // Sun Jan 23 00:00:00 +0000 2011
  private static final DateFormat dFmt = new SimpleDateFormat(
      "EEE MMM dd HH:mm:ss ZZZZZ yyyy");
  
  public static Long extractTimestamp(String html, long id) {
    Preconditions.checkNotNull(html);
    Matcher matcher = TIMESTAMP_PATTERN.matcher(html);
    Long result = null;
    
    // // most tweets have a single timestamp in the HTML
    // // retweets ONLY have the retweet timestamp in the HTML
    // // replies have both the reply tweet and the original tweet's
    // timestamps in
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
      String timeStamp = matcher.group(1);
      result = dFmt.parse(timeStamp).getTime();
    } catch (ParseException e) {
      LOG.error(e.getMessage(), e);
      result = null;
    } catch (IllegalStateException e) {
      LOG.error(e.getMessage(), e);
      result = null;
    }
    return result;
  }
  
}
