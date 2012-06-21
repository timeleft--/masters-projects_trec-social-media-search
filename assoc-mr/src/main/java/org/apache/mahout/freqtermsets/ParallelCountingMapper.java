/**
 * w * Licensed to the Apache Software Foundation (ASF) under one or more
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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Parameters;

import ca.uwaterloo.twitter.TokenIterator;
import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;
import edu.umd.cloud9.io.pair.PairOfStringLong;

/**
 * 
 * maps all items in a particular transaction like the way it is done in Hadoop
 * WordCount example
 * 
 */
public class ParallelCountingMapper extends
    Mapper<PairOfStringLong, Text, Text, LongWritable> {
  
  private static final LongWritable ONE = new LongWritable(1);
  private boolean repeatHashTag;
  private long intervalStart;
  private long intervalEnd;
  private long windowSize;
  private long endTimestamp;
  
  // private Pattern splitter;
  
  @Override
  protected void map(PairOfStringLong key, Text input, Context context)
      throws IOException, InterruptedException {
    
    String screenname = key.getLeftElement();
    long timestamp = key.getRightElement();
    if (timestamp < intervalStart) {
      return;
    } else if (timestamp > endTimestamp) {
      // I won't assume that the sequential order is reliable
      // is it possible for a mapper to say its the end anyway?
      return;
    }
    // String[] items = splitter.split(input.toString());
    LatinTokenIterator items = new LatinTokenIterator("@" + screenname + ": " + input);
    items.setRepeatHashTag(repeatHashTag);
    while (items.hasNext()) {
      String item = items.next();
      // if (item.trim().isEmpty()) {
      // continue;
      // }
      context.setStatus("Parallel Counting Mapper: " + item);
      context.write(new Text(item), ONE);
    }
  }
  
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS,
        ""));
    repeatHashTag = Boolean.parseBoolean(params.get(TokenIterator.PARAM_REPEAT_HASHTAG, "false"));
    // splitter = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN,
    // PFPGrowth.SPLITTER.toString()));
    intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START,
        Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP)));
    intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END,
        Long.toString(Long.MAX_VALUE)));
    windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    endTimestamp = Math.min(intervalEnd, intervalStart + windowSize);
  }
}
