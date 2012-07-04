package ca.uwaterloo.twitter.queryexpand;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
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
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
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
import ca.uwaterloo.twitter.queryexpand.FISQueryExpander.QueryParseMode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ibm.icu.text.SimpleDateFormat;

public class FISQueryExpanderEvaluation implements Callable<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(FISQueryExpanderEvaluation.class);
  
  private File fisIncIxLocation = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/assoc-mr_0608-0530/index");
  private File twtIncIxLoc = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/index-stemmed_8hr-incremental");
  // "/u2/yaboulnaga/datasets/twitter-trec2011/index-tweets_8hr-increments");
  
  private static final String TWT_CHUNKS_ROOT = "/u2/yaboulnaga/datasets/twitter-trec2011/index-stemmed_chunks";
  // "/u2/yaboulnaga/datasets/twitter-trec2011/index-tweets_chunks";
  private static final String RESULT_PATH = "/u2/yaboulnaga/datasets/twitter-trec2011/runs/"; // bm25-param-tune";
  private static final String TOPICS_XML_PATH =
      "/u2/yaboulnaga/datasets/twitter-trec2011/2011.topics.MB1-50.xml";
  private static final String QREL_PATH = "/u2/yaboulnaga/datasets/twitter-trec2011/microblog11-qrels.txt";
  
  private int numItemsetsToConsider = 100;
  private int numTermsToAppend = 10;
  private final boolean trecEvalFormat = true;
  private boolean paramNormalize = true;
  private boolean paramClosedOnly = true;
  private boolean paramPropagateItemSetScores = false;
  private boolean paramsBoostSubsets = false;
  private boolean paramSubsetBoostIDF = true;
  private QueryParseMode paramQueryParseMode = QueryParseMode.DISJUNCTIVE;
  private boolean paramParseToTermQueries = true;
  private boolean paramClusteringWeightIDF = false;
  private int paramNumEnglishStopWords = 3;
  
  private static final int LOG_TOP_COUNT = 30;
  
  private static final String TAG_BASELINE = "baseline";
  private static final String TAG_QUERY_CONDPROB = "qCondProb";
  private static final String TAG_KL_DIVER = "klDiver";
  private static final String TAG_CLUSTER = "clusters";
  
  private static final boolean SORT_TOPICS_CHRONOLOGICALLY = false;
  
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
  public class TrecResultFileCollector extends BM25Collector {
    
    final String runTag;
    final String topicId;
    
    public TrecResultFileCollector(String pTopicId, String pRunTag,
        String pQueryStr, OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen)
        throws IOException {
      super(target, pQueryStr, pQueryTerms, pQueryLen);
      runTag = pRunTag;
      
      if (trecEvalFormat) {
        topicId = Integer.parseInt(pTopicId.substring(2)) + "";
      } else {
        topicId = pTopicId;
      }
      
      LOG.info("========== QID: {} ============", topicId);
      LOG.info("RunTag: {} - Query: {}", runTag, queryStr);
    }
    
    public void writeResults() throws IOException {
      int rank = 0;
      Writer wr = resultWriters.get(runTag);
      for (ScoreIxObj<String> tweet : resultSet.keySet()) {
        float score = (paramNormalize ? ((tweet.score - minScore) / (maxScore - minScore))
            : tweet.score);
        wr.append(topicId).append(' ')
            .append(trecEvalFormat ? "0 " : "")
            .append(tweet.obj).append(' ')
            .append(trecEvalFormat ? rank + " " : "")
            .append("" + score).append(' ')
            .append(runTag).append('\n');
        
        if (rank < LOG_TOP_COUNT && LOG.isDebugEnabled()) {
          LOG.debug(rank + "\t" + resultSet.get(tweet) + "\t" + tweet.score + "\t" + tweet.obj);
        }
        ++rank;
      }
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
      Element queryElt = topic.getChild("title");
      if (queryElt == null) {
        // 2012 format
        queryElt = topic.getChild("query");
      }
      queries.add(queryElt.getText());
      String topicId = topic.getChildText("num");
      topicId = topicId.substring(topicId.indexOf(':') + 1).trim();
      topicIds.add(topicId);
      queryTimes.add(topic.getChildText("querytime"));
      maxTweetIds.add(topic.getChildText("querytweettime"));
    }
    
    qrelUtil = new QRelUtil(new File(QREL_PATH));
    
    twtChunkIxLocs = new File(TWT_CHUNKS_ROOT).listFiles();
    Arrays.sort(twtChunkIxLocs);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }
  
  @Before
  public void setUp() throws Exception {
    resultWriters = Maps.newHashMap();
    resultFiles = Maps.newHashMap();
    
    openWriterForTag(TAG_BASELINE);
    // openWriterForTag(TAG_QUERY_CONDPROB);
    // openWriterForTag(TAG_KL_DIVER);
    // openWriterForTag(TAG_CLUSTER);
  }
  
  private void openWriterForTag(String runTag) throws IOException {
    File resultFile = new File(RESULT_PATH, runTag + "_bm25-b" + BM25Collector.B + "-k" + BM25Collector.K1 + "-lavg" + BM25Collector.LAVG
        + "_i" + numItemsetsToConsider + "-t" + numTermsToAppend
        + "_closed" + paramClosedOnly + "-prop" + paramPropagateItemSetScores + "-subsetidf"
        + (paramsBoostSubsets && paramSubsetBoostIDF) + "-parseMode" + paramQueryParseMode
        + "-parseTQ" + paramParseToTermQueries + "-stop" + paramNumEnglishStopWords);
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
    
    if (resultFiles != null) {
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
      FilteredQuery timedQuery = null;
      Query untimedQuery = null;
      TrecResultFileCollector collector;
      
      if (resultWriters.containsKey(TAG_BASELINE)) {
        queryTerms = new OpenObjectFloatHashMap<String>();
        queryLen = new MutableLong();
        
        // if(clarity){
        // untimedQuery = target.twtQparser.parse("*:*");
        // queryTerms = target.queryTermFreq(queryStr, queryLen);
        // }else {
        untimedQuery = target.parseQuery(queryStr,
            queryTerms,
            queryLen,
            target.twtQparser,
            paramQueryParseMode, paramsBoostSubsets,paramNumEnglishStopWords);
        timedQuery = target.filterQuery(untimedQuery);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_BASELINE, queryStr, queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        
        collector.writeResults();
      } else {
        queryLen = new MutableLong();
        queryTerms = target.queryTermFreq(queryStr, queryLen);
        for(int s=0; s<paramNumEnglishStopWords; ++s){
          queryTerms.put(target.stopWordsEN[s],1);
        }
      }
      // ////////////////////////////////////////
      
      // ///////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_QUERY_CONDPROB)) {
        PriorityQueue<ScoreIxObj<String>> extraTerms;
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        extraTerms = target.convertResultToWeightedTermsConditionalProb(fis,
            queryStr,
            numItemsetsToConsider,
            paramPropagateItemSetScores,
            minXTermScore, maxXTermScore, null);
        
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new PriorityQueue[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_QUERY_CONDPROB, queryStr, queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      // /////////////////////////////////////////////////////////////
      
      if (resultWriters.containsKey(TAG_KL_DIVER)) {
        PriorityQueue<ScoreIxObj<String>> extraTerms;
        MutableFloat minXTermScore = new MutableFloat();
        MutableFloat maxXTermScore = new MutableFloat();
        
        extraTerms = target.convertResultToWeightedTermsKLDivergence(fis,
            queryStr,
            numItemsetsToConsider,
            paramPropagateItemSetScores,
            minXTermScore, maxXTermScore, null);
        
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            new PriorityQueue[] { extraTerms },
            new float[] { minXTermScore.intValue() },
            new float[] { maxXTermScore.intValue() },
            numTermsToAppend);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_QUERY_CONDPROB, queryStr, queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      
      // ///////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_CLUSTER)) {
        List<MutableFloat> minXTermScores = Lists.newArrayList();
        List<MutableFloat> maxXTermScores = Lists.newArrayList();
        List<MutableFloat> totalXTermScores = Lists.newArrayList();
        
        PriorityQueue<ScoreIxObj<String>>[] clustersTerms = target
            .convertResultToWeightedTermsByClustering(fis, queryStr, paramClosedOnly,
                minXTermScores, maxXTermScores, totalXTermScores,paramClusteringWeightIDF);
        
        timedQuery = target.expandAndFilterQuery(queryTerms,
            queryLen.intValue(),
            clustersTerms,
            minXTermScores.toArray(new MutableFloat[0]),
            maxXTermScores.toArray(new MutableFloat[0]),
            numTermsToAppend);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_CLUSTER, queryStr, queryTerms, queryLen.intValue());
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
    
    public GridSearchCollector(String pTopicId, String pRunTag, String pQueryStr,
        OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen, float pB, float pK1)
        throws IOException {
      super(pTopicId, pRunTag, pQueryStr, pQueryTerms, pQueryLen);
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
    float getB() {
      return myB;
    }
    
    @Override
    float getK1() {
      return myK1;
    }
    
    @Override
    float getLAVG() {
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
    FilteredQuery timedQuery = null;
    GridSearchCollector collector;
    
    queryTerms = new OpenObjectFloatHashMap<String>();
    queryLen = new MutableLong();
    timedQuery = target.filterQuery(target.parseQuery(queryStr,
        queryTerms,
        queryLen,
        target.twtQparser, paramQueryParseMode, paramsBoostSubsets,paramNumEnglishStopWords));
    
    for (float b : Arrays.asList(0.0f, 1.0f, 0.5f, 0.3f, 0.7f, 0.2f, 0.6f, 0.1f, 0.8f, 0.9f)) {
      // = 0.0f; b < 3; b += 0.05) {
      for (float k : Arrays.asList(0.0f, 1000f, 0.25f, 0.75f, 1.20f, 2.0f, 7f, 33f, 99f)) {
        // = 0.0f; k < 3; k += 0.05) {
        
        collector = new GridSearchCollector(topicIds.get(topicIx),
            "grid-search", queryStr, queryTerms, queryLen.intValue(), b, k);
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
    
    ExecutorService exec = Executors.newFixedThreadPool(3);
    Future<Void> lastFuture = null;
    for (int i = 5; i < topicIds.size(); i += 5) {
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
