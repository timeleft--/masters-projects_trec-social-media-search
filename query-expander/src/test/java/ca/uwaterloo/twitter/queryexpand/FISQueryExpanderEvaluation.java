package ca.uwaterloo.twitter.queryexpand;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.NumericRangeFilter;
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

import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ibm.icu.text.SimpleDateFormat;

public class FISQueryExpanderEvaluation {
  private static final Logger LOG = LoggerFactory.getLogger(FISQueryExpanderEvaluation.class);
  
  private static final float K1 = 1.2f;
  private static final float B = 0.75f;
  private static final float LAVG = 7;
  
  private File fisIncIxLocation = new File(
      "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_0608-0530/index");
  private File twtIncIxLoc = new File(
      "/u2/yaboulnaga/datasets/twitter-trec2011/index-tweets_8hr-increments");
  private static final String TWT_CHUNKS_ROOT = "/u2/yaboulnaga/datasets/twitter-trec2011/index-tweets_chunks";
  private static final String RESULT_PATH = "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/waterloo.clark";
  private int numItemsetsToConsider = 777;
  private int numTermsToAppend = 33;
  private final boolean trecEvalFormat = true;
  
  private static final String TAG_BASELINE = "baseline";
  private static final String TAG_QUERY_CONDPROB = "qCondProb";
  private static final String TAG_KL_DIVER = "klDiver";
  
  private File[] twtChunkIxLocs;
  private Map<String, Writer> resultWriters;
  
  static List<String> queries;
  static List<String> topicIds;
  static List<String> queryTimes;
  static List<String> maxTweetIds;
  
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
    final Writer wr;
    final String runTag;
    final String topicId;
    final String queryStr;
    final OpenObjectFloatHashMap<String> queryTerms;
    final int queryLen;
    
    public TrecResultFileCollector(String pTopicId, String pRunTag,
        String pQueryStr, OpenObjectFloatHashMap<String> pQueryTerms, int pQueryLen)
        throws IOException {
      wr = resultWriters.get(pRunTag);
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
      
      LOG.info("RunTag: {} - Query {}", runTag, queryStr);
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
    
    @Override
    public void collect(int docId) throws IOException {
      float luceneScore = scorer.score();
      
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
        float wt = target.twtIxReader.docFreq(t);
        wt = target.twtIxReader.numDocs() / wt;
        wt = (float) MathUtils.log(2, wt);
        
        bm25 += (queryTerms.get(tStr) * ftd * (K1 + 1) * wt)
            / ((K1 * ((1 - B) + (B * dl / LAVG))) + ftd);
      }
      
      String tweetId = doc.get(TweetField.ID.name);
      wr.append(topicId).append(' ')
          .append(trecEvalFormat ? "0 " : "")
          .append(tweetId).append(' ')
          .append(trecEvalFormat ? "0 " : "")
          .append("" + bm25).append(' ')
          .append(runTag).append('\n');
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
    
  }
  
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SAXBuilder docBuild = new SAXBuilder();
    org.jdom2.Document topicsXML = docBuild.build(new File(
        "/u2/yaboulnaga/Shared/datasets/twitter-trec2011/2011.topics.MB1-50.xml"));
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
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }
  
  @Before
  public void setUp() throws Exception {
    resultWriters = Maps.newHashMap();
    
    resultWriters.put(TAG_BASELINE, openWriterForTag(TAG_BASELINE));
    resultWriters.put(TAG_QUERY_CONDPROB, openWriterForTag(TAG_QUERY_CONDPROB));
    resultWriters.put(TAG_KL_DIVER, openWriterForTag(TAG_KL_DIVER));
    
    twtChunkIxLocs = new File(TWT_CHUNKS_ROOT).listFiles();
    Arrays.sort(twtChunkIxLocs);
    
  }
  
  private Writer openWriterForTag(String runTag) throws IOException {
    File resultFile = new File(RESULT_PATH + "_i" + numItemsetsToConsider + "_t" + numTermsToAppend
        + "_" + runTag + ".txt");
    if (resultFile.exists()) {
      // throw new IllegalArgumentException("The result file already exists.. won't overwrite");
      FileUtils
          .moveFile(resultFile,
              new File(resultFile.getAbsolutePath() + ".bak"
                  + new SimpleDateFormat("yyyMMddHHmmss").format(new Date())));
    }
    return Channels.newWriter(FileUtils.openOutputStream(resultFile).getChannel(), "UTF-8");
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
  }
  
  @Test
  public void test() throws Exception {
    
    for (int i = 0; i < queries.size(); ++i) {
      String queryStr = queries.get(i);
      target = new FISQueryExpander(fisIncIxLocation, twtIncIxLoc, twtChunkIxLocs,
          queryTimes.get(i));
      // target.twtQparser.setDefaultOperator(Operator.OR);
      OpenIntFloatHashMap fis = target.relatedItemsets(queryStr, Integer.MIN_VALUE);
      
      // Query allDocsQuery = target.twtQparser.parse("*:*");
      NumericRangeFilter<Long> timeFilter;
      // if(TASK_ADHOC){
      timeFilter = NumericRangeFilter.newLongRange(TweetField.TIMESTAMP.name,
          Long.MIN_VALUE,
          target.queryTime,
          true,
          true);
      
      // Allegedely faster to use a term query
      // Query query = target.twtQparser.parse(queryStr);
      
      OpenObjectFloatHashMap<String> queryTerms = new OpenObjectFloatHashMap<String>();
      MutableLong queryLen = new MutableLong();
      BooleanQuery query = target.parseQuery(queryStr, queryTerms, queryLen, false);
      FilteredQuery timedQuery = new FilteredQuery(query,
          timeFilter);
      
      target.twtSearcher.search(timedQuery, new TrecResultFileCollector(topicIds.get(i),
          TAG_BASELINE, queryStr, queryTerms, queryLen.intValue()));
      
      // ////////////////////////////////////////
      
      StringBuilder expanedQueryStr;
      // Query expandedQuery;
      PriorityQueue<ScoreIxObj<String>> extraTerms;
      int t;
      
      // ///////////////////////////////////////////////////////
      
      extraTerms = target.convertResultToWeightedTermsConditionalProb(fis,
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
      query = target.parseQuery(expanedQueryStr.toString(), queryTerms, queryLen, false);
      timedQuery = new FilteredQuery(query, timeFilter);
      
      target.twtSearcher.search(timedQuery, new TrecResultFileCollector(topicIds.get(i),
          TAG_QUERY_CONDPROB, expanedQueryStr.toString(), queryTerms, queryLen.intValue()));
      
      // /////////////////////////////////////////////////////////////
      
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
      query = target.parseQuery(expanedQueryStr.toString(), queryTerms, queryLen, false);
      timedQuery = new FilteredQuery(query, timeFilter);
      
      target.twtSearcher.search(timedQuery, new TrecResultFileCollector(topicIds.get(i),
          TAG_KL_DIVER, expanedQueryStr.toString(), queryTerms, queryLen.intValue()));
    }
  }
}
