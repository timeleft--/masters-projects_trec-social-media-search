package ca.uwaterloo.twitter.queryexpand;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Scorer;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ibm.icu.text.SimpleDateFormat;

public class FISQueryExpanderEvaluation {
  private static final Logger LOG = LoggerFactory.getLogger(FISQueryExpanderEvaluation.class);
  
  private static final float K1 = 100f; // default 1.2f;
  private static final float B = 0.0f; // default 0.75
  private static final float LAVG = 7;
  
  private File fisIncIxLocation = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/assoc-mr_0608-0530/index");
  private File twtIncIxLoc = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/index-tweets_8hr-increments");
  private static final String TWT_CHUNKS_ROOT = "/u2/yaboulnaga/datasets/twitter-trec2011/index-tweets_chunks";
  private static final String RESULT_PATH = "/u2/yaboulnaga/datasets/twitter-trec2011/waterloo.clark";
  private static final String TOPICS_XML_PATH =
      "/u2/yaboulnaga/datasets/twitter-trec2011/2011.topics.MB1-50.xml";
  private static final String QREL_PATH = "/u2/yaboulnaga/datasets/twitter-trec2011/microblog11-qrels.txt";
  
  private int numItemsetsToConsider = 777;
  private int numTermsToAppend = 33;
  private final boolean trecEvalFormat = true;
  private boolean paramNormalize = true;
  private boolean paramClosedOnly = true;
  private boolean paramPropagateItemSetScores = false;
  
  private static final int LOG_TOP_COUNT = 30;
  
  private static final String TAG_BASELINE = "baseline";
  private static final String TAG_QUERY_CONDPROB = "qCondProb";
  private static final String TAG_KL_DIVER = "klDiver";
  private static final String TAG_CLUSTER = "clusters";
  
  private File[] twtChunkIxLocs;
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
  public class TrecResultFileCollector extends Collector {
    
    Scorer scorer;
    IndexReader reader;
    int docBase;
    final String runTag;
    final String topicId;
    final String queryStr;
    final OpenObjectFloatHashMap<String> queryTerms;
    final int queryLen;
    final TreeMap<ScoreIxObj<String>, String> resultSet;
    int rank = 1;
    
    public TrecResultFileCollector(String pTopicId, String pRunTag,
        String pQueryStr, OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen)
        throws IOException {
      runTag = pRunTag;
      
      if (trecEvalFormat) {
        topicId = Integer.parseInt(pTopicId.substring(2)) + "";
      } else {
        topicId = pTopicId;
      }
      
      queryStr = pQueryStr;
      
      // MutableLong qLenOut = new MutableLong();
      // queryTerms = target.queryTermFreq(queryStr, qLenOut);
      // queryLen = qLenOut.intValue();
      queryTerms = pQueryTerms;
      queryLen = pQueryLen;
      
      resultSet = Maps.<ScoreIxObj<String>, String> newTreeMap();
      LOG.info("RunTag: {} - Query: {}", runTag, queryStr);
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
    
    @Override
    public void collect(int docId) throws IOException {
      // float luceneScore = scorer.score();
      
      Document doc = reader.document(docId);
      
      // TODO Store terms instead of reparsing the whole index
      // TermFreqVector docTermsVector = reader.getTermFreqVector(docId,
      // TweetField.TEXT.name);
      // OpenObjectIntHashMap<String> docTerms = new OpenObjectIntHashMap<String>();
      // int ld = 0;
      // for (int i = 0; i < docTermsVector.size(); ++i) {
      // int f = docTermsVector.getTermFrequencies()[i];
      // docTerms.put(docTermsVector.getTerms()[i], f);
      // ld += f;
      // }
      String tweet = doc.get(TweetField.TEXT.name);
      MutableLong docLen = new MutableLong();
      OpenObjectFloatHashMap<String> docTerms = target.queryTermFreq(tweet, docLen);
      float dl = docLen.floatValue();
      
      float bm25 = 0;
      for (String tStr : queryTerms.keys()) {
        float ftd = docTerms.get(tStr);
        if (ftd == 0) {
          continue;
        }
        Term t = new Term(TweetField.TEXT.name, tStr);
        
        // weight of term is its IDF
        // Also using the IDF formula in http://nlp.uned.es/~jperezi/Lucene-BM25/ for consistency
//        float idf = target.twtIxReader.docFreq(t);
//        idf = target.twtIxReader.numDocs() / idf;
//        idf = (float) MathUtils.log(2, idf);
        float idf = target.twtIxReader.docFreq(t);
        idf = (target.twtIxReader.numDocs() - idf  + 0.5f) / (idf + 0.5f);
        idf = (float) MathUtils.log(2, idf);
        
        //this formula is uncontrollable in the way I want, because I want to make tf negligible
//        bm25 += (queryTerms.get(tStr) * ftd  * idf * (K1 + 1)) 
//            / ((K1 * ((1 - B) + (B * dl / LAVG))) + ftd);
        // The BM25F formula as per http://nlp.uned.es/~jperezi/Lucene-BM25/
        float wt = ftd / ((1-B) + (B * dl / LAVG));
        
        bm25 += (wt * idf) / (K1 + wt);
      }
      
      String tweetId = doc.get(TweetField.ID.name);
      String text = null;
      if (LOG.isDebugEnabled()) {
        text = doc.get(TweetField.TEXT.name);
      }
      resultSet.put(new ScoreIxObj<String>(tweetId, bm25), text);
      
    }
    
    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      this.reader = reader;
      this.docBase = docBase;
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
    
    public void writeResults() throws IOException {
      int r = 1;
      float maxScore = resultSet.keySet().iterator().next().score;
      Writer wr = resultWriters.get(runTag);
      for (ScoreIxObj<String> tweet : resultSet.keySet()) {
        float score = (paramNormalize ? (tweet.score / maxScore) : tweet.score);
        wr.append(topicId).append(' ')
            .append(trecEvalFormat ? "0 " : "")
            .append(tweet.obj).append(' ')
            .append(trecEvalFormat ? r++ + " " : "")
            .append("" + score).append(' ')
            .append(runTag).append('\n');
        
        if (rank <= LOG_TOP_COUNT && LOG.isDebugEnabled()) {
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
    List<Element> topicElts = topicsXML.getRootElement().getChildren();
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
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }
  
  @Before
  public void setUp() throws Exception {
    resultWriters = Maps.newHashMap();
    resultFiles = Maps.newHashMap();
    
    openWriterForTag(TAG_BASELINE);
    openWriterForTag(TAG_QUERY_CONDPROB);
    // openWriterForTag(TAG_KL_DIVER);
    // openWriterForTag(TAG_CLUSTER);
    
    twtChunkIxLocs = new File(TWT_CHUNKS_ROOT).listFiles();
    Arrays.sort(twtChunkIxLocs);
    
  }
  
  private void openWriterForTag(String runTag) throws IOException {
    File resultFile = new File(RESULT_PATH + "_i" + numItemsetsToConsider + "_t" + numTermsToAppend
        + "_" + runTag + ".txt");
    if (resultFile.exists()) {
      // throw new IllegalArgumentException("The result file already exists.. won't overwrite");
      FileUtils
          .moveFile(resultFile,
              new File(resultFile.getAbsolutePath() + ".bak"
                  + new SimpleDateFormat("yyyMMddHHmmss").format(new Date())));
    }
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
  
  @Test
  public void test() throws Exception {
    
    for (int i = 0; i < queries.size(); ++i) {
      String queryStr = queries.get(i);
      target = new FISQueryExpander(fisIncIxLocation, twtIncIxLoc, twtChunkIxLocs,
          queryTimes.get(i));
      OpenIntFloatHashMap fis = target.relatedItemsets(queryStr, Integer.MIN_VALUE);
      
      // Allegedely faster to use a term query
      // target.twtQparser.setDefaultOperator(Operator.OR);
      // Query query = target.twtQparser.parse(queryStr);
      
      OpenObjectFloatHashMap<String> queryTerms;
      MutableLong queryLen;
      FilteredQuery timedQuery;
      TrecResultFileCollector collector;
      
      if (resultWriters.containsKey(TAG_BASELINE)) {
        queryTerms = new OpenObjectFloatHashMap<String>();
        queryLen = new MutableLong();
        timedQuery = target.parseQuery(queryStr, null, queryTerms, queryLen, false);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_BASELINE, queryStr, queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        
        collector.writeResults();
      }
      // ////////////////////////////////////////
      
      StringBuilder expanedQueryStr;
      PriorityQueue<ScoreIxObj<String>> extraTerms;
      int t;
      
      // ///////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_QUERY_CONDPROB)) {
        extraTerms = target.convertResultToWeightedTermsConditionalProb(fis,
            queryStr,
            numItemsetsToConsider,
            paramPropagateItemSetScores);
        expanedQueryStr = new StringBuilder(queryStr);
        
        t = 0;
        while (!extraTerms.isEmpty() && t < numTermsToAppend) {
          ScoreIxObj<String> xTerm = extraTerms.poll();
          expanedQueryStr.append(" " + xTerm.obj);
          ++t;
        }
        
        // query = target.twtQparser.parse(expanedQueryStr.toString());
        
        queryTerms = new OpenObjectFloatHashMap<String>();
        queryLen = new MutableLong();
        timedQuery = target.parseQuery(expanedQueryStr.toString(),
            null,
            queryTerms,
            queryLen,
            false);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_QUERY_CONDPROB, expanedQueryStr.toString(), queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      // /////////////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_KL_DIVER)) {
        extraTerms = target.convertResultToWeightedTermsKLDivergence(fis,
            queryStr,
            numItemsetsToConsider,
            true);
        expanedQueryStr = new StringBuilder(queryStr);
        
        t = 0;
        while (!extraTerms.isEmpty() && t < numTermsToAppend) {
          ScoreIxObj<String> xTerm = extraTerms.poll();
          expanedQueryStr.append(" " + xTerm.obj);
          ++t;
        }
        
        // query = target.twtQparser.parse(expanedQueryStr.toString());
        
        queryTerms = new OpenObjectFloatHashMap<String>();
        queryLen = new MutableLong();
        timedQuery = target.parseQuery(expanedQueryStr.toString(),
            null,
            queryTerms,
            queryLen,
            false);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_KL_DIVER, expanedQueryStr.toString(), queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
      // ///////////////////////////////////////////////////////
      if (resultWriters.containsKey(TAG_CLUSTER)) {
        PriorityQueue<ScoreIxObj<String>>[] clustersTerms = target
            .convertResultToWeightedTermsByClustering(fis, paramClosedOnly);
        expanedQueryStr = new StringBuilder(queryStr);
        
        t = 0;
        while (t < numTermsToAppend) {
          for (int c = 0; c < clustersTerms.length; ++c, ++t) {
            if (clustersTerms[c].isEmpty()) {
              continue;
            }
            ScoreIxObj<String> xTerm = clustersTerms[c].poll();
            expanedQueryStr.append(" " + xTerm.obj);
          }
        }
        
        // query = target.twtQparser.parse(expanedQueryStr.toString());
        
        queryTerms = new OpenObjectFloatHashMap<String>();
        queryLen = new MutableLong();
        timedQuery = target.parseQuery(expanedQueryStr.toString(),
            null,
            queryTerms,
            queryLen,
            false);
        
        collector = new TrecResultFileCollector(topicIds.get(i),
            TAG_CLUSTER, expanedQueryStr.toString(), queryTerms, queryLen.intValue());
        target.twtSearcher.search(timedQuery, collector);
        collector.writeResults();
      }
    }
  }
}
