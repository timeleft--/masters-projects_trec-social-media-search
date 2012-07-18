/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.map.OpenObjectLongHashMap;

/**
 * 
 * groups all Frequent Patterns containing an item and outputs the top K patterns
 * containing that particular item
 * 
 */
public class AggregatorReducer extends Reducer<Text, TopKStringPatterns, Text, TopKStringPatterns> {
//  public static final char METADATA_PREFIX = '_';
  public static final String MUTUAL_INFO_FLAG = "mutualInfo";
  private int maxHeapSize = 50;
  private final OpenObjectLongHashMap<String> fMap = new OpenObjectLongHashMap<String>();
  
  private double totalNterms;
  private boolean sortByMutualInfo;
  private double lnTotalNTerms;
  
  @Override
  protected void reduce(Text key, Iterable<TopKStringPatterns> values, Context context)
      throws IOException,
      InterruptedException {
    
    // YA get data to do more than freq merge
    int myMaxHeapSize = maxHeapSize;
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf); //TODO: do I need?getLocal(conf);
    String cachePath = FilenameUtils.concat(FileUtils.getTempDirectory().toURI().toString(), Thread
        .currentThread().getName() + "_" + key.hashCode() + "_patterns");
    org.apache.hadoop.io.ArrayFile.Writer cacheWr = new ArrayFile.Writer(conf, fs, cachePath,
        TopKStringPatterns.class);
    final String keyStr = key.toString();
    final OpenObjectLongHashMap<String> jointFreq = new OpenObjectLongHashMap<String>();
    
    TopKStringPatterns metaPatterns = new TopKStringPatterns();
    
    for (TopKStringPatterns value : values) {
      
      List<Pair<List<String>, Long>> vPatterns = value.getPatterns();
      for(int p = vPatterns.size() - 1; p >= 0; --p){
        Pair<List<String>, Long> pattern = vPatterns.get(p);
        if (pattern == null) {
          continue; // just like their merge
        }
        for (String other : pattern.getFirst()) {
          if(other.charAt(0) == ' '){ //METADATA_PREFIX){
            // Keep metadata out of merge
            vPatterns.remove(p);
            
            // Make sure it has space to be merged
            ++myMaxHeapSize;
            
            // Store the metadata temporarily.. we will add it in the end
            // where it can't be pruned out
            metaPatterns.getPatterns().add(pattern);
            
            // done processing metadata itemset
            break;
          }
          if (keyStr.equals(other)) {
            continue;
          }
          long freq = jointFreq.get(other);
          if (pattern.getSecond() > freq) {
            freq = pattern.getSecond();
          }
          jointFreq.put(other, freq);
        }
      }
      
      cacheWr.append(value);
    }
    cacheWr.close();
    
    org.apache.hadoop.io.ArrayFile.Reader cacheRd = new ArrayFile.Reader(fs, cachePath, conf);
    // END YA get data
    
    TopKStringPatterns patterns = new TopKStringPatterns();
    TopKStringPatterns value = new TopKStringPatterns();
    while (cacheRd.next(value) != null) {
      context.setStatus("Aggregator Reducer: Selecting TopK patterns for: " + key);
      
      // YA Mutual info merge.. TODO: more metrics passed as class name of comparator
      if (sortByMutualInfo) {
        patterns = patterns.merge(value, myMaxHeapSize, new Comparator<Pair<List<String>, Long>>() {
          
          private double calcNormalizedMutualInfo(String[] bagOfTokens) {
            double numer = 0;
            double denim = 0;
            double ft1 = fMap.get(keyStr);
            for (int t2 = 0; t2 < bagOfTokens.length; ++t2) {
              if (bagOfTokens[t2].equals(keyStr)) {
                continue;
              }
              double ft2 = fMap.get(bagOfTokens[t2]);
              double jf = jointFreq.get(bagOfTokens[t2]);
              
              // This check shouldn't be even plausible.. save time:
              // if(jf != 0){
              double jp = jf / totalNterms;
              
              numer += jp * (Math.log(jf / (ft1 * ft2)) + lnTotalNTerms);
              
              denim += jp * Math.log(jp);
            }
            
            double result = numer;
            if (denim != 0) {
              result /= -denim;
            }
            return result;
          }
          
          @Override
          public int compare(Pair<List<String>, Long> o1, Pair<List<String>, Long> o2) {
            String[] bagOfTokens = o1.getFirst().toArray(new String[0]);
            
            double mi1 = calcNormalizedMutualInfo(bagOfTokens);
            
            bagOfTokens = o2.getFirst().toArray(new String[0]);
            
            double mi2 = calcNormalizedMutualInfo(bagOfTokens);
            
            int result = Double.compare(mi1, mi2);
            if (result == 0) {
              result = Double.compare(o1.getFirst().size(), o2.getFirst().size());
              
              if (result == 0) {
                result = o1.getSecond().compareTo(o2.getSecond());
              }
            }
            return result;
          }
        });
        // END YA Mutual info merge
      } else {
        patterns = patterns.mergeFreq(value, myMaxHeapSize);
      }
    }
    
    // YA get data
    cacheRd.close();
    fs.delete(new Path(cachePath), true);
    
    patterns = patterns.merge(metaPatterns, myMaxHeapSize, new Comparator<Pair<List<String>,Long>>() {
      @Override
      public int compare(Pair<List<String>, Long> o1, Pair<List<String>, Long> o2) {
        // Force the metadata to be accepted
        return -1;
      }
    });
    // END YA get data
    
    context.write(key, patterns);
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    Parameters params = new Parameters(conf.get(PFPGrowth.PFP_PARAMETERS, ""));
    maxHeapSize = Integer.valueOf(params.get(PFPGrowth.MAX_HEAPSIZE, "50"));
    
//    totalNterms = 0;
//    for (Pair<String, Long> e : PFPGrowth.readFList(conf)) {
//      fMap.put(e.getFirst(), e.getSecond());
//      totalNterms += e.getSecond();
//    }
    
    totalNterms = PFPGrowth.readFMap(conf,fMap);
    
    lnTotalNTerms = Math.log(totalNterms);
    sortByMutualInfo = "true".equals(params.get(MUTUAL_INFO_FLAG));
  }
}
