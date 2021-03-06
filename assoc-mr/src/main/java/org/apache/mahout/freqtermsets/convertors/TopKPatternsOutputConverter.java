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

package org.apache.mahout.freqtermsets.convertors;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.fpgrowth.FrequentPatternMaxHeap;
import org.apache.mahout.freqtermsets.fpgrowth.Pattern;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * An output converter which converts the output patterns and collects them in a
 * {@link FrequentPatternMaxHeap}
 * 
 * @param <A>
 */
public final class TopKPatternsOutputConverter<A extends Comparable<? super A>> implements
    OutputCollector<Integer, FrequentPatternMaxHeap> {
  private final OutputCollector<A, List<Pair<List<A>, Long>>> collector;
  private final OpenIntObjectHashMap<A> reverseMapping;
  
  public TopKPatternsOutputConverter(OutputCollector<A, List<Pair<List<A>, Long>>> collector,
      OpenIntObjectHashMap<A> idIxMap){
    this.collector = collector;
    this.reverseMapping = idIxMap;
  }
  
  @Override
  public void collect(Integer key, FrequentPatternMaxHeap value) throws IOException {
    List<Pair<List<A>, Long>> perAttributePatterns = Lists.newArrayList();
    PriorityQueue<Pattern> t = value.getHeap();
    while (!t.isEmpty()) {
      Pattern itemSet = t.poll();
      List<A> frequentPattern = Lists.newArrayList();
      for (int j = 0; j < itemSet.length(); j++) {
        frequentPattern.add(reverseMapping.get(itemSet.getPattern()[j]));
      }
      Collections.sort(frequentPattern);
      
      Pair<List<A>, Long> returnItemSet = new Pair<List<A>, Long>(frequentPattern,
          itemSet.support());
      perAttributePatterns.add(returnItemSet);
    }
    Collections.reverse(perAttributePatterns);
    
    collector.collect(reverseMapping.get(key), perAttributePatterns);
  }
}
