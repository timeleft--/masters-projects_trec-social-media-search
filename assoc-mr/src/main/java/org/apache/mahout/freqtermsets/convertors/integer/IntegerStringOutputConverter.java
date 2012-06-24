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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.AggregatorReducer;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.TokenIterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Collects the Patterns with Integer id and Long support and converts them to Pattern of Strings
 * based on a
 * reverse feature lookup map.
 */
public final class IntegerStringOutputConverter implements
    OutputCollector<Integer, List<Pair<List<Integer>, Long>>> {
  
  // YA langDetect
  private static final Logger log = LoggerFactory.getLogger(IntegerStringOutputConverter.class);
  
  private static final String LANGUAGE_ITEMSET_TOKEN = "LANG";
  
  // TODO: command line
  public static final boolean DO_LANG_DETECTION = false;
  // private final Pattern DIGITS_PATTERN = Pattern.compile(".*[0-9].*");
  
  private TextCategorizer langCat = new TextCategorizer();
  private int minWordsForLangDetect;
  // private final double superiorityRatio;
  // private final double superiorityRatioRecip;
  // private final Random rand = new Random(System.currentTimeMillis());
  private boolean repeatHashTag = false;
  // END YA langDetect
  
  private final OutputCollector<Text, TopKStringPatterns> collector;
  
  // private final List<String> featureReverseMap;
  OpenIntObjectHashMap<String> featureReverseMap;
  
  public IntegerStringOutputConverter(OutputCollector<Text, TopKStringPatterns> collector,
      OpenIntObjectHashMap<String> reverseMap, int pMinWordsForLangDetect, boolean pRepeatHashTag) {
    this.collector = collector;
    this.featureReverseMap = reverseMap;
    minWordsForLangDetect = pMinWordsForLangDetect;
    this.repeatHashTag = pRepeatHashTag;
  }
  
  @Override
  public void collect(Integer key, List<Pair<List<Integer>, Long>> value) throws IOException {
    String stringKey = featureReverseMap.get(key);
    
    // YA: Detect langauge for attribute by voting from different patterns
    // String langSure = null;
    int patternWords;
    // int totalSupport = 0;
    Set<String> hashtags = Sets.<String> newHashSet();
    HashMap<String, MutableLong> langVotes = Maps.<String, MutableLong> newHashMap();
    StringBuilder patStr = new StringBuilder();
    // END langDetect
    
    List<Pair<List<String>, Long>> stringValues = Lists.newArrayList();
    for (Pair<List<Integer>, Long> e : value) {
      List<String> pattern = Lists.newArrayList();
      
      if (DO_LANG_DETECTION) {
        // YA langDetect
        // totalSupport += itemSet.support();
        patStr.setLength(0);
        hashtags.clear();
        patternWords = 0;
        char k0 = stringKey.charAt(0);
        if (k0 == '#') {
          if (repeatHashTag)
            hashtags.add(stringKey.substring(1));
        } else if (!(k0 == '@' || stringKey.equals(TokenIterator.URL_PLACEHOLDER))) {
          patStr.append(stringKey);
          ++patternWords;
        }
        // END langDetect
      }
      
      for (Integer i : e.getFirst()) {
        String token = featureReverseMap.get(i);
        pattern.add(token);
        
        if (DO_LANG_DETECTION) {
          // YA langDetect
          char ch0 = token.toString().charAt(0);
          if (ch0 == '#') {
            if (repeatHashTag)
              hashtags.add(token.substring(1));
          } else if (!(ch0 == '@' || stringKey.equals(token) || token
              .equals(TokenIterator.URL_PLACEHOLDER))) {
            if (hashtags.contains(token)) {
              hashtags.remove(token);
              // This is the extra token that is returned after the hashtag
              // Hashtags are no good for language detection
              // if (DIGITS_PATTERN.matcher(token).matches()) {
              // contains digits so it is not a proper word
              // TODO: any other heuristics
              continue;
            }
            patStr.append(' ').append(token);
            ++patternWords;
          }
          
          if (patternWords >= minWordsForLangDetect) {
            String lang = langCat.categorize(patStr.toString());
            if (!langVotes.containsKey(lang)) {
              langVotes.put(lang, new MutableLong(0));
            }
            MutableLong voteCnt = langVotes.get(lang);
            voteCnt.add(e.getSecond());
            
            log.info("Detected language for attribute '{}' to be '{}'",
                stringKey,
                lang);
          }
          // END langDetect
        }
      }
      stringValues.add(new Pair<List<String>, Long>(pattern, e.getSecond()));
    }
    
    // YA langDetect
    
    // Vote for one language
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
    
    // langvotes will be empty anyway: if(DO_LANG_DETECTION)
    for (Entry<String, MutableLong> voteEntry : langVotes.entrySet()) {
      stringValues
          .add(new Pair<List<String>, Long>(Arrays.asList(stringKey,
              AggregatorReducer.METADATA_PREFIX + LANGUAGE_ITEMSET_TOKEN,
              AggregatorReducer.METADATA_PREFIX + voteEntry.getKey()), voteEntry.getValue()
              .longValue()));
    }
    // END YA langDetect
    
    collector.collect(new Text(stringKey), new TopKStringPatterns(stringValues));
  }
}
