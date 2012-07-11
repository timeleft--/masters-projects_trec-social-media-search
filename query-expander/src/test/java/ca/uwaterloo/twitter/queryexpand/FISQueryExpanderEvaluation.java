package ca.uwaterloo.twitter.queryexpand;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.Channels;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.trecutil.QRelUtil;
import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;
import ca.uwaterloo.twitter.queryexpand.FISQueryExpander.ExpandMode;
import ca.uwaterloo.twitter.queryexpand.FISQueryExpander.QueryExpansionBM25Collector;
import ca.uwaterloo.twitter.queryexpand.FISQueryExpander.QueryParseMode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ibm.icu.text.SimpleDateFormat;

public class FISQueryExpanderEvaluation implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(FISQueryExpanderEvaluation.class);
  
  public static final int MAX_RESULTS = 10000;
  private File fisIncIxLocation = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/assoc-mr_0608-0530/index-closed_stemmed-stored");
  
  private File twtIncIxLoc = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/" +
//          "index_orig/");
  // "index-stemmed_8hr-incremental");
   "stemmed-stored_8hr-increments");
  
  private static final String TWT_CHUNKS_ROOT = 
   "/u2/yaboulnaga/datasets/twitter-trec2011/"
  // + "index-stemmed_chunks";
   + "stemmed-stored_chunks";

  private static final String RESULT_PATH = "/u2/yaboulnaga/datasets/twitter-trec2011/runs/";
  private static final String TOPICS_XML_PATH =
      "/u2/yaboulnaga/datasets/twitter-trec2011/"
//          + "2012.topics.MB51-110.xml";
   + "2011.topics.MB1-50.xml";
  private static final String QREL_PATH = null;
  // "/u2/yaboulnaga/datasets/twitter-trec2011/microblog11-qrels.txt";
  
  private int numItemsetsToConsider = 100;
  private int numTermsToAppend = 10;
  private final boolean trecEvalFormat = true;
  private boolean paramNormalize = false;
  private boolean paramClosedOnly = true;
  private boolean paramPropagateItemSetScores = true;
  private boolean paramsBoostSubsets = false;
  private boolean paramSubsetBoostIDF = true;
  private QueryParseMode paramQueryParseMode = QueryParseMode.DISJUNCTIVE;
  private boolean paramParseToTermQueries = true;
  private boolean paramClusteringWeight = true;
  private int paramNumEnglishStopWords = 0;
  private boolean paramBM25StemmedIDF = true;
  private boolean paramMarkovProbDocFromTwitter = false;
  
  private static final int LOG_TOP_COUNT = 30;
  private static final boolean SORT_TOPICS_CHRONOLOGICALLY = false;
  
  // save myself the pain of removing duplicate documents (FIXME: why are they still appearing)
  // And hopefully increase the MAP without decreasing recall (requires paramNormalize = true)
  private static final float SCORE_THRESHOLD = Float.MIN_VALUE;
  
  private static final boolean SORT_RESULTS_REVERSE_CHRONOLOGICALLY = false;
  
  private static final String TAG_BASELINE = "baseline";
  private static final String TAG_FREQ_PATTERNS = "freqPatterns";
  private static final String TAG_FROM_TWEETS = "fromTweets";
  private static final String TAG_TOPN = "nFromTopPatterns";
  private static final String TAG_QUERY_CONDPROB = "qCondProb";
  private static final String TAG_KL_DIVER = "klDiver";
  private static final String TAG_CLUSTER_PATTERNS = "clusterPatterns";
  private static final String TAG_CLUSTER_TWEETS = "clusterTweets";
  private static final String TAG_CLUSTER_TERMS = "clusTerm";
  private static final String TAG_MARKOV = "markov";
  private static final String TAG_SVD_PATTERN = "svd-pattern";
  private static final String TAG_MUTUALINF = "mutualInf";
  private static final String TAG_CONDENTR = "condEntr";
  
  private static File[] twtChunkIxLocs;
  
  private Map<String, Writer> resultWriters;
  private Map<String, File> resultFiles;
  
  static List<String> queries;
  static List<String> topicIds;
  static List<String> queryTimes;
  static List<String> maxTweetIds;
  static QRelUtil qrelUtil;
  
  FISQueryExpander target;
  
  /**
   * query_id, iter, docno, rank, sim, run_id
   * 
   * @author yaboulna
   * 
   */
  public class TrecResultFileCollector extends QueryExpansionBM25Collector {
    final String runTag;
    final String topicId;
    
    public TrecResultFileCollector(FISQueryExpander pTarget, String pTopicId, String pRunTag,
       /* String pQueryStr,*/ OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen)
        throws IOException, IllegalArgumentException, SecurityException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
      // (paramBM25StemmedIDF ? TweetField.STEMMED_EN.name : TweetField.TEXT.name),
      super(pTarget, TweetField.TEXT.name,
         /* pQueryStr,*/ pQueryTerms, pQueryLen,
          paramNumEnglishStopWords, MAX_RESULTS,
          (Class<? extends Comparator<ScoreIxObj<String>>>) ScoreThenObjDescComparator.class,
          paramBM25StemmedIDF);
      runTag = pRunTag;
      
      if (trecEvalFormat) {
        topicId = Integer.parseInt(pTopicId.substring(2)) + "";
      } else {
        topicId = pTopicId;
      }
      
      LOG.info("========== QID: {} ============", topicId);
      LOG.info("RunTag: {} - Query: {}", runTag, queryTerms);//queryStr);
    }
    
    public void writeResults() throws IOException {
      int rank = 0;
      Writer wr = resultWriters.get(runTag);
      TreeMap<ScoreIxObj<String>, String> rs = resultSet;
      if (SORT_RESULTS_REVERSE_CHRONOLOGICALLY) {
        rs = new TreeMap<ScoreIxObj<String>, String>(new Comparator<ScoreIxObj<String>>() {
          
          @Override
          public int compare(ScoreIxObj<String> o1, ScoreIxObj<String> o2) {
            return -o1.obj.compareTo(o2.obj);
          }
          
        });
        for (ScoreIxObj<String> scoredTweet : resultSet.keySet()) {
          rs.put(scoredTweet, resultSet.get(scoredTweet));
        }
      }
      
      for (ScoreIxObj<String> scoredTweet : rs.keySet()) {
        String tweet = null;
        if (languageIdentification || rank < LOG_TOP_COUNT && LOG.isDebugEnabled()) {
          tweet = resultSet.get(scoredTweet);
          LOG.debug(rank + "\t" + tweet + "\t" + scoredTweet.score + "\t" + scoredTweet.obj);
        }
        if (languageIdentification) {
          String lang = FISQueryExpander.textcat.categorize(tweet);
          if (!"english".equals(lang)) {
            LOG.debug("Neglecting ({}) tweet: {}", lang, tweet);
            continue;
          }
        }
        
        float score = (paramNormalize ? ((scoredTweet.score - minScore) / (maxScore - minScore))
            : scoredTweet.score);
        if (paramNormalize && score <= SCORE_THRESHOLD) {
          continue;
        }
        wr.append(topicId).append(' ')
            .append(trecEvalFormat ? "0 " : "")
            .append(scoredTweet.obj).append(' ')
            .append(trecEvalFormat ? rank + " " : "")
            .append("" + score).append(' ')
            .append(runTag).append('\n');
        
        ++rank;
      }
      wr.flush();
    }
  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SAXBuilder docBuild = new SAXBuilder();
    org.jdom2.Document topicsXML = docBuild.build(new File(TOPICS_XML_PATH));
    
    List<Element> topicElts = Lists.newArrayListWithCapacity(50);
    for (Element elt : topicsXML.getRootElement().getChildren()) {
      topicElts.add(elt.clone().detach());
    }
    
    if (SORT_TOPICS_CHRONOLOGICALLY) {
      Collections.sort(topicElts, new Comparator<Element>() {
        SimpleDateFormat dFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
        
        @Override
        public int compare(Element o1, Element o2) {
          try {
            long t1 = dFmt.parse(o1.getChildText("querytime")).getTime();
            long t2 = dFmt.parse(o2.getChildText("querytime")).getTime();
            return Double.compare(t1, t2);
          } catch (ParseException e) {
            LOG.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    }
    
    queries = Lists.newArrayListWithCapacity(topicElts.size());
    topicIds = Lists.newArrayListWithCapacity(topicElts.size());
    queryTimes = Lists.newArrayListWithCapacity(topicElts.size());
    maxTweetIds = Lists.newArrayListWithCapacity(topicElts.size());
    for (Element topic : topicElts) {
      String topicId = topic.getChildText("num");
      topicId = topicId.substring(topicId.indexOf(':') + 1).trim();
//      if (topicId.compareTo("MB076") != 0) { // MB076 is the funny one
//        continue;
//      }
      topicIds.add(topicId);
      
      Element queryElt = topic.getChild("title");
      if (queryElt == null) {
        // 2012 format
        queryElt = topic.getChild("query");
      }
      queries.add(queryElt.getText());
      
      queryTimes.add(topic.getChildText("querytime"));
      maxTweetIds.add(topic.getChildText("querytweettime"));
    }
    
    if (QREL_PATH != null) {
      qrelUtil = new QRelUtil(new File(QREL_PATH));
    }
    if (TWT_CHUNKS_ROOT != null) {
      twtChunkIxLocs = new File(TWT_CHUNKS_ROOT).listFiles();
      Arrays.sort(twtChunkIxLocs);
    } else {
      twtChunkIxLocs = null;
    }
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }
  
  @Before
  public void setUp() throws Exception {
    resultWriters = Maps.newHashMap();
    resultFiles = Maps.newHashMap();
    
//    openWriterForTag(TAG_BASELINE);
    // openWriterForTag(TAG_FROM_TWEETS);
    // openWriterForTag(TAG_FREQ_PATTERNS);
    // openWriterForTag(TAG_FREQ_PATTERNS + 50);
    // openWriterForTag(TAG_FREQ_PATTERNS + 77);
    // openWriterForTag(TAG_FREQ_PATTERNS + 100);
    // openWriterForTag(TAG_FREQ_PATTERNS + 333);
//    openWriterForTag(TAG_TOPN);
    // openWriterForTag(TAG_QUERY_CONDPROB);
    // openWriterForTag(TAG_KL_DIVER);
    openWriterForTag(TAG_CLUSTER_PATTERNS);
//    openWriterForTag(TAG_CLUSTER_TWEETS);
    // openWriterForTag(TAG_CLUSTER_TERMS);
    // openWriterForTag(TAG_MARKOV);
    // openWriterForTag(TAG_SVD_PATTERN);
    // openWriterForTag(TAG_MUTUALINF);
    // openWriterForTag(TAG_CONDENTR);
    
  }
  
  private void openWriterForTag(String runTag) throws IOException {
    File resultFile = new File(RESULT_PATH, runTag + "_bm25-b" + BM25Collector.B + "-k"
        + BM25Collector.K1 + "-lavg" + BM25Collector.LAVG
        + "_i" + numItemsetsToConsider + "-t" + numTermsToAppend
        + "_closed" + paramClosedOnly + "-prop" + paramPropagateItemSetScores + "-subsetidf"
        + (paramsBoostSubsets && paramSubsetBoostIDF) + "-parseMode" + paramQueryParseMode
        + "-parseTQ" + paramParseToTermQueries + "-stop" + paramNumEnglishStopWords + "-stemmedIDF"
        + paramBM25StemmedIDF + "-mpdw" + paramMarkovProbDocFromTwitter);
    if (resultFile.exists()) {
      // throw new IllegalArgumentException("The result file already exists.. won't overwrite");
      FileUtils
          .moveDirectory(resultFile,
              new File(resultFile.getAbsolutePath() + "_bak-before-"
                  + new SimpleDateFormat("MMddHHmmss").format(new Date())));
    }
    resultFile = new File(resultFile, new SimpleDateFormat("MMddHHmmss").format(new Date())
        + ".txt");
    resultWriters.put(runTag,
        Channels.newWriter(FileUtils.openOutputStream(resultFile).getChannel(), "UTF-8"));
    resultFiles.put(runTag, resultFile);
  }
  
  @After
  public void tearDown() throws Exception {
    if (target != null) {
      target.close();
    }
    
    if (resultWriters != null) {
      for (Writer resultWr : resultWriters.values()) {
        resultWr.flush();
        resultWr.close();
      }
    }
    
    if (qrelUtil != null && resultFiles != null) {
      for (File resultF : resultFiles.values()) {
        Writer unjWr = Channels.newWriter(FileUtils.openOutputStream(new File(resultF
            .getAbsolutePath() + ".unjudged")).getChannel(),
            "UTF-8");
        try {
          Map<String, List<String>> unjudged = qrelUtil.findUnjedged(resultF, LOG_TOP_COUNT);
          for (String qid : unjudged.keySet()) {
            unjWr.append("Qid: " + qid + "\n");
            unjWr.append("Num. Unjudged: " + unjudged.get(qid).size() + "\n");
            unjWr.append("Unjudged Ids: " + unjudged.get(qid) + "\n");
          }
        } finally {
          unjWr.flush();
          unjWr.close();
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void test() throws Exception {
    
    for (int i = 0; i < queries.size(); ++i) {
      String queryStr = queries.get(i);
      target = new FISQueryExpander(fisIncIxLocation, twtIncIxLoc, twtChunkIxLocs,
          queryTimes.get(i));
      // target.setBoostQuerySubsets(paramsScoreSubsets);
      target.setBoostQuerySubsetByIdf(paramSubsetBoostIDF);
      target.setParseToTermQueries(paramParseToTermQueries);
      
      OpenIntFloatHashMap fis;
      if (resultWriters.size() == 1 && resultWriters.containsKey(TAG_BASELINE)) {
        fis = null;
      } else {
        fis = target.relatedItemsets(queryStr, Integer.MIN_VALUE);
      }
      
      // Allegedely faster to use a term query
      // target.twtQparser.setDefaultOperator(Operator.OR);
      // Query query = target.twtQparser.parse(queryStr);
      
      OpenObjectFloatHashMap<String> queryTerms;
      MutableLong queryLen;
      queryLen = new MutableLong();
      queryTerms = target
          .queryTermFreq(queryStr,
              queryLen,
              (FISQueryExpander.SEARCH_NON_STEMMED ? FISQueryExpander.tweetNonStemmingAnalyzer
                  : FISQueryExpander.tweetStemmingAnalyzer),
              (FISQueryExpander.SEARCH_NON_STEMMED ? TweetField.TEXT.name
                  : TweetField.STEMMED_EN.name));
      // FISQueryExpander.tweetStemmingAnalyzer,
      // TweetField.STEMMED_EN.name);
      // FISQueryExpander.tweetNonStemmingAnalyzer,
      // TweetField.TEXT.name);
      
      Query timedQuery = null;
      Query untimedQuery = null;
      TrecResultFileCollector collector;
      
      if (resultWriters.containsKey(TAG_BASELINE)) {
        // queryTerms = new OpenObjectFloatHashMap<String>();
        // queryLen = new MutableLong();
        
        // if(clarity){
        // untimedQuery = target.twtQparser.parse("*:*");
        // queryTerms = target.queryTermFreq(queryStr, queryLen);
        // }else {
        // untimedQuery = target.parseQuery(queryStr,
        // queryTerms,
        // queryLen,
        // target.twtQparser,
        // paramQueryParseMode, paramsBoostSubsets);
        untimedQuery = target.parseQueryIntoTerms(queryTerms,
            queryLen.floatValue(),
            paramQueryParseMode,
            paramsBoostSubsets,
            // TweetField.STEMMED_EN.name,
            TweetField.TEXT.name,
            target.twtIxReader);
        timedQuery = target.filterQuery(untimedQuery);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_BASELINE, //queryStr, 
            queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        
        collector.writeResults();
        
        // / Run in a Run //////////////////////////////////////////////////
        if (resultWriters.containsKey(TAG_FROM_TWEETS)) {
          
          MutableFloat minXTermScore = new MutableFloat();
          MutableFloat maxXTermScore = new MutableFloat();
          
          OpenObjectFloatHashMap<String> extraTerms = collector
              .expansionTermsByIDFIncrease(numItemsetsToConsider,
                  minXTermScore,
                  maxXTermScore,
                  null);
          
          OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
          MutableLong xQueryLen = new MutableLong(0);
          timedQuery = target.expandAndFilterQuery(queryTerms,
              queryLen.intValue(),
              new OpenObjectFloatHashMap[] { extraTerms },
              new float[] { minXTermScore.intValue() },
              new float[] { maxXTermScore.intValue() },
              numTermsToAppend,
              xQueryTerms, xQueryLen, ExpandMode.DIVERSITY); // FILTERING
          
          collector = new TrecResultFileCollector(target, topicIds.get(i),
              TAG_FROM_TWEETS, //queryStr,
              xQueryTerms, xQueryLen.intValue());
          target.twtSearcher.search(timedQuery, collector);
          collector.writeResults();
        }
        
        // / Another run in a run ///////////////////////////////
        if (resultWriters.containsKey(TAG_CLUSTER_TWEETS)) {
          List<MutableFloat> minXTermScores = Lists.newArrayList();
          List<MutableFloat> maxXTermScores = Lists.newArrayList();
          List<MutableFloat> totalXTermScores = Lists.newArrayList();
          
          PriorityQueue<ScoreIxObj<String>>[] clusters = collector
              .expansionTermsByClusterTopResults(
                  numItemsetsToConsider,
                  minXTermScores,
                  maxXTermScores,
                  totalXTermScores, queryStr);
          OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
          MutableLong xQueryLen = new MutableLong(0);
          timedQuery = target.expandAndFilterQuery(queryTerms,
              queryLen.intValue(),
              clusters,
              minXTermScores.toArray(new MutableFloat[0]),
              maxXTermScores.toArray(new MutableFloat[0]),
              numTermsToAppend,
              xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
          
          collector = new TrecResultFileCollector(target, topicIds.get(i),
              TAG_CLUSTER_TWEETS, //queryStr,
              xQueryTerms, xQueryLen.intValue());
          target.twtSearcher.search(timedQuery, collector);
          collector.writeResults();
        }
      }
      // ////////////////////////////////////////
      
      // /////////////////////////////////////////////////////////////
      // // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_FREQ_PATTERNS)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsByFreq(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScore,
            maxXTermScore,
            null,
            paramPropagateItemSetScores);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_FREQ_PATTERNS, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_FREQ_PATTERNS + 3)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsByFreq(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScore,
            maxXTermScore,
            null,
            paramPropagateItemSetScores);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            3, // numTermsToAppend,
            xQueryTerms,
            xQueryLen,
            ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_FREQ_PATTERNS + 3, //queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
        
        xQueryTerms = new OpenObjectFloatHashMap<String>();
        xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            7, // numTermsToAppend,
            xQueryTerms,
            xQueryLen,
            ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_FREQ_PATTERNS + 7,// queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
        
        xQueryTerms = new OpenObjectFloatHashMap<String>();
        xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            13,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_FREQ_PATTERNS + 13, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
        
        xQueryTerms = new OpenObjectFloatHashMap<String>();
        xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            33, // numTermsToAppend,
            xQueryTerms,
            xQueryLen,
            ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_FREQ_PATTERNS + 33, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_TOPN)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsByPatternRank(fis,
            queryStr,
            numTermsToAppend,
            minXTermScore,
            maxXTermScore,
            null);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_TOPN, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_MARKOV)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsMarkovChain(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScore,
            maxXTermScore,
            null,
            FISQueryExpander.PARAM_MARKOV_NUM_WALK_STEPS,
            FISQueryExpander.PARAM_MARKOV_ALPHA,
            paramMarkovProbDocFromTwitter);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_MARKOV, //queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_SVD_PATTERN)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsSVDOfPatternMatrix(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScore,
            maxXTermScore,
            null,
            paramMarkovProbDocFromTwitter);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_SVD_PATTERN, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_QUERY_CONDPROB)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsConditionalProb(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScore,
            maxXTermScore,
            null);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_QUERY_CONDPROB, //queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_KL_DIVER)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsByKLDivergence(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScore,
            maxXTermScore,
            null, paramMarkovProbDocFromTwitter);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_KL_DIVER, //queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_MUTUALINF)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsByConditionalEntropy(fis,
            queryStr,
            numItemsetsToConsider,
            numTermsToAppend,
            minXTermScore,
            maxXTermScore,
            null, false);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_MUTUALINF, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_CONDENTR)) {
        
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        OpenObjectFloatHashMap<String> extraTerms = target.weightedTermsByConditionalEntropy(fis,
            queryStr,
            numItemsetsToConsider,
            numTermsToAppend,
            minXTermScore,
            maxXTermScore,
            null, true);
        
        OpenObjectFloatHashMap<String> xQueryTerms = new OpenObjectFloatHashMap<String>();
        MutableLong xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new OpenObjectFloatHashMap[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_CONDENTR, //queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // // ////////////////////////////
      OpenObjectFloatHashMap<String> xQueryTerms;
      MutableLong xQueryLen;
      //
      // // ///////////////////////////////////////////////////////
      // if (resultWriters.containsKey(TAG_QUERY_CONDPROB)) {
      // PriorityQueue<ScoreIxObj<String>> extraTerms;
      // MutableFloat minXTermScore = new MutableFloat();
      // MutableFloat maxXTermScore = new MutableFloat();
      //
      // extraTerms = target.convertResultToWeightedTermsConditionalProb(fis,
      // queryStr,
      // numItemsetsToConsider,
      // paramPropagateItemSetScores,
      // minXTermScore, maxXTermScore, null);
      //
      // xQueryTerms = new OpenObjectFloatHashMap<String>();
      // xQueryLen = new MutableLong(0);
      // timedQuery = target.expandAndFilterQuery(queryTerms,
      // queryLen.intValue(),
      // new PriorityQueue[] { extraTerms },
      // new float[] { minXTermScore.intValue() },
      // new float[] { maxXTermScore.intValue() },
      // numTermsToAppend,
      // xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
      //
      // collector = new TrecResultFileCollector(target, topicIds.get(i),
      // TAG_QUERY_CONDPROB, queryStr, xQueryTerms, xQueryLen.intValue());
      // target.twtSearcher.search(timedQuery, collector);
      // collector.writeResults();
      // }
      // // /////////////////////////////////////////////////////////////
      //
      // if (resultWriters.containsKey(TAG_KL_DIVER)) {
      // PriorityQueue<ScoreIxObj<String>> extraTerms;
      // MutableFloat minXTermScore = new MutableFloat();
      // MutableFloat maxXTermScore = new MutableFloat();
      //
      // extraTerms = target.convertResultToWeightedTermsKLDivergence(fis,
      // queryStr,
      // numItemsetsToConsider,
      // paramPropagateItemSetScores,
      // minXTermScore, maxXTermScore, null);
      //
      // xQueryTerms = new OpenObjectFloatHashMap<String>();
      // xQueryLen = new MutableLong(0);
      // timedQuery = target.expandAndFilterQuery(queryTerms,
      // queryLen.intValue(),
      // new PriorityQueue[] { extraTerms },
      // new float[] { minXTermScore.intValue() },
      // new float[] { maxXTermScore.intValue() },
      // numTermsToAppend,
      // xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
      //
      // collector = new TrecResultFileCollector(target, topicIds.get(i),
      // TAG_KL_DIVER, queryStr, xQueryTerms, xQueryLen.intValue());
      // target.twtSearcher.search(timedQuery, collector);
      // collector.writeResults();
      // }
      //
      // ///////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_CLUSTER_PATTERNS)) {
        List<MutableFloat> minXTermScores = Lists.newArrayList();
        List<MutableFloat> maxXTermScores = Lists.newArrayList();
        List<MutableFloat> totalXTermScores = Lists.newArrayList();
        
        PriorityQueue<ScoreIxObj<String>>[] clusters = target.weightedTermsClusterPatterns(fis,
            queryStr,
            numItemsetsToConsider,
            minXTermScores,
            maxXTermScores,
            totalXTermScores,
            paramMarkovProbDocFromTwitter);
        xQueryTerms = new OpenObjectFloatHashMap<String>();
        xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            clusters,
            minXTermScores.toArray(new MutableFloat[0]),
            maxXTermScores.toArray(new MutableFloat[0]),
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_CLUSTER_PATTERNS, //queryStr,
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // ///////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_CLUSTER_TERMS)) {
        List<MutableFloat> minXTermScores = Lists.newArrayList();
        List<MutableFloat> maxXTermScores = Lists.newArrayList();
        List<MutableFloat> totalXTermScores = Lists.newArrayList();
        
        PriorityQueue<ScoreIxObj<String>>[] clustersTerms = target
            .weightedTermsByClusteringTerms(fis,
                queryStr,
                numItemsetsToConsider,
                minXTermScores,
                maxXTermScores,
                totalXTermScores);
        // .convertResultToWeightedTermsByClusteringTerms(fis, queryStr, paramClosedOnly,
        // minXTermScores, maxXTermScores, totalXTermScores, paramClusteringWeight);
        xQueryTerms = new OpenObjectFloatHashMap<String>();
        xQueryLen = new MutableLong(0);
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            clustersTerms,
            minXTermScores.toArray(new MutableFloat[0]),
            maxXTermScores.toArray(new MutableFloat[0]),
            numTermsToAppend,
            xQueryTerms, xQueryLen, ExpandMode.DIVERSITY);
        
        collector = new TrecResultFileCollector(target, topicIds.get(i),
            TAG_CLUSTER_TERMS, //queryStr, 
            xQueryTerms, xQueryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
    }
  }
  
  // /////////////////////////////////////////////////////
  class GridSearchCollector extends TrecResultFileCollector {
    
    final private float myB;
    final private float myK1;
    final private String writerKey;
    
    public GridSearchCollector(FISQueryExpander ptarget, String pTopicId, String pRunTag,
        //String pQueryStr,
        OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen, float pB, float pK1)
        throws IOException, IllegalArgumentException, SecurityException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
      super(ptarget, pTopicId, pRunTag, /*pQueryStr, */pQueryTerms, pQueryLen);
      myB = pB;
      myK1 = pK1;
      writerKey = "b" + myB + "k" + myK1;
      LOG.debug("BM25 b: {} - K1: {}", myB, myK1);
    }
    
    @Override
    public void writeResults() throws IOException {
      int r = 0;
      Writer wr = acquireWriter();
      try {
        for (ScoreIxObj<String> tweet : resultSet.keySet()) {
          float score = (paramNormalize ? ((tweet.score - minScore) / (maxScore - minScore))
              : tweet.score);
          wr.append(topicId).append(' ')
              .append(trecEvalFormat ? "0 " : "")
              .append(tweet.obj).append(' ')
              .append(trecEvalFormat ? r++ + " " : "")
              .append("" + score).append(' ')
              .append(runTag).append('\n');
        }
      } finally {
        
        releaseWriter(wr);
      }
    }
    
    @Override
    protected float getB() {
      return myB;
    }
    
    @Override
    protected float getK1() {
      return myK1;
    }
    
    @Override
    protected float getLAVG() {
      return super.getLAVG();
    }
    
    private Writer acquireWriter() throws IOException {
      File resultFile;
      synchronized (sharedResultFiles) {
        if (!sharedResultFiles.containsKey(writerKey)) {
          resultFile = new File(RESULT_PATH, runTag + "_bm25-b" + getB() + "-k" + getK1()
              + "-lavg" + getLAVG()
              + "_i" + numItemsetsToConsider + "-t" + numTermsToAppend
              + "_closed" + paramClosedOnly + "-prop" + paramPropagateItemSetScores);
          resultFile = new File(resultFile, new SimpleDateFormat("MMddHHmmss").format(new Date())
              + ".txt");
          
          sharedResultFiles.put(writerKey, resultFile);
          FileUtils.openOutputStream(resultFile, false).getChannel().close();
          sharedResultWritersToken.put(writerKey, new Object());
        } else {
          resultFile = sharedResultFiles.get(writerKey);
        }
      }
      
      Writer result = null;
      Object token;
      while (true) {
        synchronized (sharedResultWritersToken) {
          token = sharedResultWritersToken.remove(writerKey);
        }
        if (token != null) {
          result = Channels.newWriter(FileUtils.openOutputStream(resultFile, true).getChannel(),
              "UTF-8");
          break;
        } else {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
      return result;
    }
    
    private void releaseWriter(Writer wr) throws IOException {
      synchronized (sharedResultWritersToken) {
        wr.flush();
        wr.close();
        sharedResultWritersToken.put(writerKey, new Object());// wr);
      }
    }
  }
  
  private static Map<String, Object> sharedResultWritersToken = Collections
      .synchronizedMap(new HashMap<String, Object>());
  private static Map<String, File> sharedResultFiles = Collections
      .synchronizedMap(new HashMap<String, File>());
  private int topicIx = -1;
  
  public FISQueryExpanderEvaluation(int pTopicIx) {
    topicIx = pTopicIx;
  }
  
  public FISQueryExpanderEvaluation() {
  }
  
  @Override
  public Void call() throws Exception {
    String queryStr = queries.get(topicIx);
    target = new FISQueryExpander(fisIncIxLocation, twtIncIxLoc, twtChunkIxLocs,
        queryTimes.get(topicIx));
    
    OpenObjectFloatHashMap<String> queryTerms;
    MutableLong queryLen;
    Query timedQuery = null;
    GridSearchCollector collector;
    
    queryLen = new MutableLong();
    // queryTerms = new OpenObjectFloatHashMap<String>();
    // timedQuery = target.filterQuery(target.parseQuery(queryStr,
    // queryTerms,
    // queryLen,
    // target.twtQparser, paramQueryParseMode, paramsBoostSubsets));
    queryTerms = target.queryTermFreq(queryStr,
        queryLen,
        (FISQueryExpander.SEARCH_NON_STEMMED ? FISQueryExpander.tweetNonStemmingAnalyzer
            : FISQueryExpander.tweetStemmingAnalyzer),
        (FISQueryExpander.SEARCH_NON_STEMMED ? TweetField.TEXT.name : TweetField.STEMMED_EN.name));
    // target.tweetStemmingAnalyzer,
    // TweetField.STEMMED_EN.name);
    // FISQueryExpander.tweetNonStemmingAnalyzer,
    // TweetField.TEXT.name);
    Query untimedQuery = target.parseQueryIntoTerms(queryTerms,
        queryLen.floatValue(),
        paramQueryParseMode,
        paramsBoostSubsets,
        (FISQueryExpander.SEARCH_NON_STEMMED ? TweetField.TEXT.name : TweetField.STEMMED_EN.name),
        // TweetField.STEMMED_EN.name,
        // TweetField.TEXT.name,
        target.twtIxReader);
    timedQuery = target.filterQuery(untimedQuery);
    
    for (float b : Arrays.asList(0.0f,
        0.03f,
        0.07f,
        0.12f,
        1.0f,
        0.5f,
        0.3f,
        0.7f,
        0.2f,
        0.6f,
        0.1f,
        0.8f,
        0.9f)) {
      // = 0.0f; b < 3; b += 0.05) {
      for (float k : Arrays.asList(0.0f,
          0.66f,
          0.77f,
          0.9f,
          1.0f,
          1.1f,
          1.3f,
          1.5f,
          1000f,
          0.25f,
          0.05f,
          1.20f,
          2.0f,
          7f,
          33f,
          99f)) {
        // = 0.0f; k < 3; k += 0.05) {
        
        collector = new GridSearchCollector(target, topicIds.get(topicIx),
            "grid-search", 
            //queryStr, 
            queryTerms, queryLen.intValue(), b, k);
        target.twtSearcher.search(timedQuery, collector);
        
        collector.writeResults();
      }
    }
    if (target != null) {
      target.close();
    }
    return null;
  }
  
  public static void main(String[] args) throws Exception {
    
    setUpBeforeClass();
    
    ExecutorService exec = Executors.newFixedThreadPool(10);
    Future<Void> lastFuture = null;
    for (int i : Arrays.asList(5, 9, 10, 15, 20, 25, /* 30, */35, 40, 45)) {
      FISQueryExpanderEvaluation app = new FISQueryExpanderEvaluation(i);
      lastFuture = exec.submit(app);
    }
    
    lastFuture.get();
    exec.shutdown();
    while (!exec.isTerminated()) {
      Thread.sleep(1000);
    }
    
    // synchronized (sharedResultWriters) {
    // for (Writer resultWr : sharedResultWriters.values()) {
    // resultWr.flush();
    // resultWr.close();
    // }
    // }
    
    tearDownAfterClass();
  }
}
