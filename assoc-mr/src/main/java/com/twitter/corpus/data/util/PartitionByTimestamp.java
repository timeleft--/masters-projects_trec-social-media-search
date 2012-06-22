package com.twitter.corpus.data.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.WeakHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.PFPGrowth;
import org.apache.mahout.math.map.OpenLongObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.hadoop.util.CorpusReader;

import com.google.common.collect.Maps;
import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.data.HtmlTweetRecordReader;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class PartitionByTimestamp {
//  private static final Logger LOG = LoggerFactory.getLogger(PartitionByTimestamp.class);
  
  public static final long FILE_INTERVAL_MILLIS = 5 * 60 * 1000;
  public static final long FOLDER_INTERVAL_MILLIS = 60 * 60 * 1000;
  
  private static Map<String, List<Pair<Long, Path>>> chilrdenCache = Collections
      .synchronizedMap(new WeakHashMap<String, List<Pair<Long, Path>>>());
  
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path htmlPath = new Path("file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/html");
    CorpusReader<PairOfLongString, HtmlStatus> reader =
        new CorpusReader<PairOfLongString, HtmlStatus>(htmlPath, fs, ".*\\.html\\.seq");
    
    Path outPath = new Path(
        "file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/html_hour-5min");
    // "hdfs://scspc400.cs.uwaterloo.ca:9000/twitter2011/byhour/");
    
    HadoopUtil.delete(conf, outPath);
    
    OpenLongObjectHashMap<Path> folders = new OpenLongObjectHashMap<Path>();
    Map<Path, Writer> writers = Maps.newHashMap();
    try {
      Pair<PairOfLongString, HtmlStatus> record;
      while ((record = reader.next()) != null) {
        long id = record.getFirst().getLeftElement();
        HtmlStatus htmlStatus = record.getSecond();
        if (htmlStatus.getHttpStatusCode() >= 300) {
          continue;
        }
        Long timestamp = HtmlTweetRecordReader.extractTimestamp(htmlStatus.getHtml(), id);
        if (timestamp == null) {
          continue;
        }
        long hourstamp = getHourTimestamp(timestamp);
        
        if (!folders.containsKey(hourstamp)) {
          Path folder = new Path(outPath, "" + hourstamp);
          folders.put(hourstamp, folder);
          long fileend = hourstamp + FILE_INTERVAL_MILLIS;
          long folderend = hourstamp + FOLDER_INTERVAL_MILLIS;
          while (fileend <= folderend) { // <= in creation, because it is not inclusive
            FileSystem.create(fs, new Path(folder, "" + fileend), FsPermission.getDefault());
            fileend += FILE_INTERVAL_MILLIS;
          }
        }
        
        Path hourFolder = folders.get(hourstamp);
        
        Pair<Long, Path> timePath = fileForTime(hourFolder, timestamp, fs);
        
        long fileEndTime = timePath.getFirst();
        
        if (!writers.containsKey(timePath.getSecond())) {
          writers.put(timePath.getSecond(), SequenceFile.createWriter(fs, conf,
              timePath.getSecond(),
              PairOfLongString.class, HtmlStatus.class, SequenceFile.CompressionType.BLOCK));
        }
        
        Writer wr = writers.get(timePath.getSecond());
        
        long folderStartTime = Long.parseLong(hourFolder.getName());
        long folderEndTime = folderStartTime + FOLDER_INTERVAL_MILLIS;
        
        assert (timestamp >= folderStartTime && timestamp < folderEndTime && timestamp < fileEndTime) : "Timestamp "
            + timestamp + " doesn't fit in the prepared folder/file " +
            folderEndTime + "/" + fileEndTime;
        
        wr.append(record.getFirst(), record.getSecond());
      }
    } finally {
      reader.close();
      
      for (Writer wr : writers.values()) {
        wr.close();
      }
    }
  }
  
  private static long getHourTimestamp(Long timestamp) {
    GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
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
  // List<Pair<Long, Path>> timeList = getChildrenTimeList(parent, startOrEnd, fs);
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
  
  private static Pair<Long, Path> fileForTime(Path hourFolder, long exactTime, FileSystem fs)
      throws IOException {
    Pair<Long, Path> result = null;
    for (Pair<Long, Path> timeFile : getChildrenTimeList(hourFolder, false, fs)) {
      if (timeFile.getFirst() > exactTime) {
        result = timeFile;
      } else {
        break;
      }
    }
    return result;
  }
  
  private static List<Pair<Long, Path>> getChildrenTimeList(Path parent,
      boolean ascendingOrDescending,
      FileSystem fs) throws IOException {
    int direction = ascendingOrDescending ? 1 : -1;
    List<Pair<Long, Path>> timeList;
    String cacheKey = parent.toString() + direction;
    if (chilrdenCache.containsKey(cacheKey)) {
      timeList = chilrdenCache.get(cacheKey);
    } else {
      FileStatus[] children = fs.listStatus(parent);
      timeList = new ArrayList<Pair<Long, Path>>(children.length);
      
      int i = (ascendingOrDescending ? 0 : children.length - 1);
      while (i >= 0 && i < children.length) {
        FileStatus child = children[i];
        long childTime = Long.parseLong(child.getPath().getName());
        timeList.add(new Pair<Long, Path>(childTime, child.getPath()));
        i += direction;
      }
      chilrdenCache.put(cacheKey, timeList);
    }
    return timeList;
  }
  
  public static void setInputPaths(Job job, Parameters params, FileSystem fs) throws IOException {
    
    long startTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)));
    long endTime = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE)));
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(endTime - startTime)));
    endTime = Math.min(endTime, startTime + windowSize - 1);
    
    Path parent = new Path(params.get(PFPGrowth.INPUT));
    long rhs = endTime + FILE_INTERVAL_MILLIS;
    while (startTime < endTime) {
      long hourstamp = getHourTimestamp(startTime);
      Pair<Long, Path> folder = new Pair<Long, Path>(hourstamp, new Path(parent, hourstamp + ""));
      assert (folder.getFirst() >= startTime && folder.getFirst() < endTime) : "Folder "
          + folder.getSecond() + " is not suitable for the interval " + startTime
          + "-" + endTime;
      List<Pair<Long, Path>> children = getChildrenTimeList(folder.getSecond(), true, fs);
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
  
}
