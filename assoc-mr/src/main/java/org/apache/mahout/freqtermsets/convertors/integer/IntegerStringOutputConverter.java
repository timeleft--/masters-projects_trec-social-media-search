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

package org.apache.mahout.freqtermsets.convertors.integer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Collects the Patterns with Integer id and Long support and converts them to Pattern of Strings
 * based on a
 * reverse feature lookup map.
 */
public final class IntegerStringOutputConverter implements
    OutputCollector<Integer, List<Pair<List<Integer>, Long>>> {

  //YA langDetect
  private static final Logger log = LoggerFactory.getLogger(IntegerStringOutputConverter.class);

  private TextCategorizer langCat = new TextCategorizer();
  private int minAccompWordsForLangDetect;
//  private final double superiorityRatio;
//  private final double superiorityRatioRecip;
//  private final Random rand = new Random(System.currentTimeMillis());
  // END YA langDetect

  private final OutputCollector<Text, TopKStringPatterns> collector;
  
  private final List<String> featureReverseMap;
  
  public IntegerStringOutputConverter(OutputCollector<Text, TopKStringPatterns> collector,
      List<String> featureReverseMap, int pMinAccompWordsForLangDetect) {
    this.collector = collector;
    this.featureReverseMap = featureReverseMap;
    minAccompWordsForLangDetect = pMinAccompWordsForLangDetect;
  }
  
  @Override
  public void collect(Integer key, List<Pair<List<Integer>,Long>> value) throws IOException {
    // YA: Detect langauge for attribute by voting from different patterns
    String langSure = null;
    int accompanyingWords = 0;
//    int totalSupport = 0;
    StringBuilder patStr = new StringBuilder();
    // END langDetect

    String stringKey = featureReverseMap.get(key);
    List<Pair<List<String>,Long>> stringValues = Lists.newArrayList();
    for (Pair<List<Integer>,Long> e : value) {
      List<String> pattern = Lists.newArrayList();
      
      // YA langDetect
      accompanyingWords += e.getFirst().size() - 1;
//      totalSupport += itemSet.support();
      // patStr.setLength(0);
      // END langDetect
      
      for (Integer i : e.getFirst()) {
        String token = featureReverseMap.get(i);
        pattern.add(token);

        // YA langDetect
          char ch0 = token.toString().charAt(0);
          if (ch0 == '#' || ch0 == '@') {
            // continue;
          } else {
            patStr.append(token).append(' ');
          }
        // END langDetect
      }
      stringValues.add(new Pair<List<String>,Long>(pattern, e.getSecond()));
    }

 // YA langDetect
    if (accompanyingWords >= minAccompWordsForLangDetect) {
      // HashMap<String, MutableLong> langVotes = Maps.newHashMap();
      // int majorityVoteCnt = (frequentPatterns.count() / 2) + 1;
      // for (Pattern p : frequentPatterns.getHeap()) {
      
      // String lang =
      langSure = langCat.categorize(patStr.toString());
      
      // if (!langVotes.containsKey(lang)) {
      // langVotes.put(lang, new MutableLong(0));
      // }
      // MutableLong voteCnt = langVotes.get(lang);
      // voteCnt.add(1);
      //
      // if (voteCnt.longValue() == majorityVoteCnt) {
      // langSure = lang;
      // break;
      // }
      // }
      // if (frequentPatterns.count() > 0 && langSure == null) {
      //
      // double maxVote = 1e-9;
      // List<String> langCandidates = Lists.newLinkedList();
      //
      // for (Entry<String, MutableLong> voteEntry : langVotes.entrySet()) {
      // double ratio = voteEntry.getValue().doubleValue() / maxVote;
      // if (ratio >= superiorityRatio) {
      // langCandidates.clear();
      // }
      //
      // if (ratio > superiorityRatioRecip) {
      // langCandidates.add(voteEntry.getKey());
      // }
      //
      // if (ratio > 1.0) {
      // maxVote = voteEntry.getValue().intValue();
      // }
      // }
      //
      // if (langCandidates.size() > 0) {
      // langSure = langCandidates.get(rand.nextInt(langCandidates.size()));
      // } else {
      // log.warn("Language Candidates is empty! Using 'unknown'. freqPatterns: {}, langVotes: {}",
      // frequentPatterns.getHeap(),
      // langVotes);
      // langSure = "unknown";
      // }
      // }
    } else {
      langSure = "unknown";
    }
    
    log.info("Detected language for attribute '{}' to be '{}'",
        stringKey,
        langSure);
    
    // TODO: support more than one language
    if ("english".equals(langSure) || "unknown".equals(langSure)) {
      collector.collect(new Text(stringKey), new TopKStringPatterns(stringValues));
    }
    // END YA langDetect

//    collector.collect(new Text(stringKey), new TopKStringPatterns(stringValues));
  }
}
