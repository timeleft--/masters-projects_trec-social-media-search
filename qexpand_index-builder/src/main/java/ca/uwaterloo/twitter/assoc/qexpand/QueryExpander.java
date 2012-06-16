package ca.uwaterloo.twitter.assoc.qexpand;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.TransactionTree;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class QueryExpander {
  private static Logger LOG = LoggerFactory.getLogger(QueryExpander.class);
  
  private static final String FIS_INDEX_OPTION = "fis_index";
  private static final String TWT_INDEX_OPTION = "twt_index";
  
  // private static final String BASE_PARAM_OPTION = "base_param";
  private static final float FIS_BASE_RANK_PARAM_DEFAULT = 60.0f;
  
  private static final String MIN_SCORE_OPTION = "min_score";
  private static final float MIN_SCORE_DEFAULT = Float.MIN_VALUE; // very sensitive!
  
  // Must be large, because otherwise the topic drifts quickly...
  // it seems that the top itemsets are repetetive, so we need more breadth
  private static final int NUM_HITS_INTERNAL_DEFAULT = 777;
  // This is enough recall and the concept doesn't drift much.. one more level pulls trash..
  // less is more precise, but I'm afraid won't have enough recall; will use score to block trash
  private static final int MAX_LEVELS_EXPANSION = 3;
  private static final int NUM_HITS_SHOWN_DEFAULT = 1000;
  
  private static final Analyzer ANALYZER = new ItemsetAnalyzer();// new
                                                                 // EnglishAnalyzer(Version.LUCENE_36);
  private static final boolean CHAR_BY_CHAR = false;
  
  private static final int MIN_ITEMSET_SIZE = 1;
  
  private static final String RETWEET_QUERY = "RT";
  
  private static final float ITEMSET_LEN_AVG_DEFAULT = 5;
  
  private static final float ITEMSET_LEN_WEIGHT_DEFAULT = 0.33f;
  
  private static final float ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT = 0.77f;
  
  private static final float TERM_WEIGHT_SMOOTHER_DEFAULT = 0.33f;
  
  private static final float SCORE_PRECISION_MULTIPLIER = 100000;
  
  public static enum TweetField {
    ID("id"),
    SCREEN_NAME("screen_name"),
    CREATED_AT("create_at"),
    TEXT("text"),
    DAY("day");
    
    public final String name;
    
    TweetField(String s) {
      name = s;
    }
  };
  
  /**
   * @param args
   * @throws IOException
   * @throws org.apache.lucene.queryParser.ParseException
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("frequent itemsets index location").create(FIS_INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("twitter index location").create(TWT_INDEX_OPTION));
    // options.addOption(OptionBuilder.withArgName("float").hasArg()
    // .withDescription("parameter that would be used to rank top level queries and reduced in lower levels").create(BASE_PARAM_OPTION));
    options.addOption(OptionBuilder.withArgName("float").hasArg()
        .withDescription("score for accepting a document into the resultset")
        .create(MIN_SCORE_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    
    if (!(cmdline.hasOption(FIS_INDEX_OPTION) && cmdline.hasOption(TWT_INDEX_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(QueryExpander.class.getName(), options);
      System.exit(-1);
    }
    
    File fisIndexLocation = new File(cmdline.getOptionValue(FIS_INDEX_OPTION));
    if (!fisIndexLocation.exists()) {
      System.err.println("Error: " + fisIndexLocation + " does not exist!");
      System.exit(-1);
    }
    
    File twtIndexLocation = new File(cmdline.getOptionValue(TWT_INDEX_OPTION));
    if (!twtIndexLocation.exists()) {
      System.err.println("Error: " + twtIndexLocation + " does not exist!");
      System.exit(-1);
    }
    
    // float baseBoost = BASE_PARAM_DEFAULT;
    // try {
    // if (cmdline.hasOption(BASE_PARAM_OPTION)) {
    // baseBoost = Float.parseFloat(cmdline.getOptionValue(BASE_PARAM_OPTION));
    // }
    // } catch (NumberFormatException e) {
    // System.err.println("Invalid " + BASE_PARAM_OPTION + ": "
    // + cmdline.getOptionValue(BASE_PARAM_OPTION));
    // System.exit(-1);
    // }
    
    float minScore = MIN_SCORE_DEFAULT;
    try {
      if (cmdline.hasOption(MIN_SCORE_OPTION)) {
        minScore = Float.parseFloat(cmdline.getOptionValue(MIN_SCORE_OPTION));
      }
    } catch (NumberFormatException e) {
      System.err.println("Invalid " + MIN_SCORE_OPTION + ": "
          + cmdline.getOptionValue(MIN_SCORE_OPTION));
      System.exit(-1);
    }
    
    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    QueryExpander qEx = null;
    try {
      qEx = new QueryExpander(fisIndexLocation, twtIndexLocation);
      
      StringBuilder query = new StringBuilder();
      do {
        if (CHAR_BY_CHAR) {
          int ch = System.in.read();
          if (ch == '\r' || ch == '\n') {
            query.setLength(0);
            continue;
          }
          query.append((char) ch);
        } else {
          query.setLength(0);
          BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
          query.append(reader.readLine());
        }
        
        if (query.length() == 0) {
          continue;
        }
        
        int mode = -1;
        String cmd = query.substring(0, 2);
        if (cmd.equals("i:")) {
          mode = 0; // itemsets
        } else if (cmd.equals("t:")) {
          mode = 5; // terms
        } else if (cmd.equals("r:")) {
          mode = 10; // results
        } else if (cmd.equals("e:")) {
          mode = 20; // expanded
        } else {
          out.println("Must prefix either i: or r: or e:");
          continue;
        }
        
        query.delete(0, 2);
        
        out.println();
        out.println(">" + query.toString());
        OpenIntFloatHashMap fisRs = null;
        if (mode != 10) {
          fisRs = qEx.relatedItemsets(query.toString(), minScore);
        }
        
        Query parsedQuery = qEx.twtQparser.parse(query.toString());
        
        if (mode == 0) {
          PriorityQueue<ScoreIxObj<List<String>>> itemsets = qEx.convertResultToItemsets(fisRs,
              query.toString(),
              -1);
          // NUM_HITS_DEFAULT);
          int i = 0;
          while (!itemsets.isEmpty()) {
            ScoreIxObj<List<String>> is = itemsets.poll();
            out.println(++i + " (" + is.score + "): " + is.obj.toString());
          }
        } else if (mode == 5) {
          OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>();
          MutableLong itemsetsLength = new MutableLong();
          
          qEx.convertResultToWeightedTerms(fisRs, query.toString(), termFreq, itemsetsLength, -1);
          
          LinkedList<String> terms = Lists.<String> newLinkedList();
          termFreq.keysSortedByValue(terms);
          Iterator<String> termsIter = terms.descendingIterator();
          int i = 0;
          while (termsIter.hasNext()) {
            String t = termsIter.next();
            
            float termWeight = termFreq.get(t);
            
            out.println(++i + " (" + termWeight + "): " + t);
          }
          
        } else {
          BooleanQuery twtQ = new BooleanQuery();
          twtQ.add(qEx.twtQparser.parse(RETWEET_QUERY).rewrite(qEx.twtIxReader), Occur.MUST_NOT);
          
          if (mode == 10) {
            twtQ.add(parsedQuery, Occur.MUST);
          } else if (mode == 20) {
            twtQ.add(qEx.convertResultToBooleanQuery(fisRs,
                query.toString(),
                NUM_HITS_INTERNAL_DEFAULT),
                Occur.SHOULD);
            twtQ.setMinimumNumberShouldMatch(1);
          }
          
          LOG.debug("Querying Twitter by: " + twtQ.toString());
          int t = 0;
          for (ScoreDoc scoreDoc : qEx.twtSearcher.search(twtQ, NUM_HITS_SHOWN_DEFAULT).scoreDocs) {
            Document hit = qEx.twtSearcher.doc(scoreDoc.doc);
            Field created = hit.getField(TweetField.CREATED_AT.name);
            out.println();
            out.println(String.format("%4d (%.4f): %s\t%s\t%s\t%s",
                ++t,
                scoreDoc.score,
                hit.getField(TweetField.TEXT.name).stringValue(),
                hit.getField(TweetField.SCREEN_NAME.name).stringValue(),
                (created == null ? "" : created.stringValue()),
                hit.getField(TweetField.ID.name).stringValue()));
            
            if (t % 100 == 0) {
              LOG.trace(t + " woohoo, here's another " + 100);
            }
          }
        }
      } while (true);
    } finally {
      if (qEx != null)
        qEx.close();
    }
  }
  
  private final QueryParser fisQparser;
  private final IndexSearcher fisSearcher;
  private final IndexReader fisIxReader;
  
  private final int fisNumHits = NUM_HITS_INTERNAL_DEFAULT;
  private final float fisBaseRankingParam = FIS_BASE_RANK_PARAM_DEFAULT;
  
  private final Similarity fisSimilarity;
  
  private final QueryParser twtQparser;
  private final IndexSearcher twtSearcher;
  private final IndexReader twtIxReader;
  
  // private final int twtNumHits = NUM_HITS_DEFAULT;
  
  private final Similarity twtSimilarity;
  
  private float itemsetLenghtAvg = ITEMSET_LEN_AVG_DEFAULT;
  
  // As in Lambda of Jelink Mercer smoothing
  private float itemsetCorpusModelWeight = ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT;
  
  // As in Mue of Dirchilet smoothing
  private float termWeightSmoother = TERM_WEIGHT_SMOOTHER_DEFAULT;
  
  private long twtCorpusLength = -1;

  private float itemSetLenWght= ITEMSET_LEN_WEIGHT_DEFAULT;
  
  public QueryExpander(File fisIndexLocation, File twtIndexLocation) throws IOException {
    Directory fisdir = new MMapDirectory(fisIndexLocation);
    fisIxReader = IndexReader.open(fisdir);
    fisSearcher = new IndexSearcher(fisIxReader);
    fisSimilarity = new ItemSetSimilariry();
    fisSearcher.setSimilarity(fisSimilarity);
    
    fisQparser = new QueryParser(Version.LUCENE_36,
        IndexBuilder.AssocField.ITEMSET.name,
        ANALYZER);
    fisQparser.setDefaultOperator(Operator.AND);
    
    Directory twtdir = new MMapDirectory(twtIndexLocation);
    twtIxReader = IndexReader.open(twtdir);
    twtSearcher = new IndexSearcher(twtIxReader);
    twtSimilarity = new TwitterSimilarity();
    twtSearcher.setSimilarity(twtSimilarity);
    
    twtQparser = new QueryParser(Version.LUCENE_36, TweetField.TEXT.name, ANALYZER);
    twtQparser.setDefaultOperator(Operator.AND);
    
    BooleanQuery.setMaxClauseCount(fisNumHits * fisNumHits);
  }
  
  public OpenIntFloatHashMap relatedItemsets(String queryStr, float minScore)
      throws IOException,
      org.apache.lucene.queryParser.ParseException {
    if (queryStr == null || queryStr.isEmpty()) {
      throw new IllegalArgumentException("Query cannot be empty");
    }
    // if (minScore <= 0) {
    // throw new IllegalArgumentException("Minimum score must be positive");
    // }
    
    Query allQuery = fisQparser.parse(queryStr);
    // allQuery.setBoost(baseBoost);
    allQuery = allQuery.rewrite(fisIxReader);
    
    Set<Term> queryTermsTemp = Sets.<Term> newHashSet();
    Set<String> queryTerms = Sets.<String> newHashSet();
    allQuery.extractTerms(queryTermsTemp);
    for (Term term : queryTermsTemp) {
      queryTerms.add(term.text());
    }
    queryTermsTemp = null;
    
    BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    // This boost should be used only for the allQuery
    // query.setBoost(baseBoost);
    query.add(allQuery, Occur.SHOULD);
    
    if (queryTerms.size() > 2) {
      String[] queryArr = queryTerms.toArray(new String[0]);
      for (int i = 0; i < queryTerms.size() - 1; ++i) {
        for (int j = i + 1; j < queryTerms.size(); ++j) {
          Query pairQuery = fisQparser.parse(queryArr[i] + " " + queryArr[j]);
          pairQuery.setBoost((2.0f / queryTerms.size())); // * baseBoost);
          query.add(pairQuery, Occur.SHOULD);
        }
      }
      queryArr = null;
    }
    
    OpenIntFloatHashMap resultSet = new OpenIntFloatHashMap();
    TopDocs rs = fisSearcher.search(query, fisNumHits);
    
    Set<ScoreIxObj<String>> extraTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int levelHits = addQualifiedResults(rs,
        resultSet,
        queryTerms,
        extraTerms,
        minScore,
        fisBaseRankingParam);
    LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    Set<ScoreIxObj<String>> doneTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int level = 1;
    while (extraTerms.size() > 0 && level < MAX_LEVELS_EXPANSION) {
      float fusionK = (float) (fisBaseRankingParam + Math.pow(10, level));
      extraTerms = expandRecursive(queryTerms,
          extraTerms,
          doneTerms,
          resultSet,
          fisNumHits,
          minScore,
          fusionK);
      ++level;
    }
    
    // for (int i = levelHits; i < rs.scoreDocs.length && result.size() < numHits; ++i) {
    // result.add(new
    // ScoreIxObj<Integer>(rs.scoreDocs[i].doc,rs.scoreDocs[i].score,rs.scoreDocs[i].shardIndex));
    // }
    
    return resultSet;
  }
  
  public void convertResultToWeightedTerms(OpenIntFloatHashMap rs,
      String query, OpenObjectFloatHashMap<String> termWeightOut,
      MutableLong itemsetsLengthOut, int numResults) throws IOException {
    
    OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
    
    TransactionTree itemsets = convertResultToItemsetsInternal(rs,
        queryTermFreq(query),
        termIds,
        itemsetsLengthOut,
        numResults);
    
    List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
    termIds.keysSortedByValue(terms);
    
    // Level traversal of the tree
    
    Queue<Integer> parentQueue = Queues.newLinkedBlockingQueue();
    parentQueue.add(TransactionTree.ROOTNODEID);
    int level=1;
    long nTotal = 0; //itemsetsLengthOut.longValue();
    while (!parentQueue.isEmpty()) {
      Integer p = parentQueue.poll();
      int children = itemsets.childCount(p);
      for (int c = 0; c < children; ++c) {
        int n = itemsets.childAtIndex(p, c);
        String t =terms.get(itemsets.attribute(n));

        // TF no IDF (commented)
        float w = (float)(itemsets.count(n)/SCORE_PRECISION_MULTIPLIER);
//            * (float)(Math.log(twtIxReader.numDocs()/(double)(docFreq+1)) + 1.0);
        termWeightOut.put(t,
            termWeightOut.get(t) + w);
        
        nTotal += w;
        
        parentQueue.add(n);
      }
      ++level;
    }
    
    List<String> weightedTerms = Lists.newArrayListWithCapacity(termWeightOut.size());
    termWeightOut.keysSortedByValue(weightedTerms);
    

    for(String t: weightedTerms){
      float pIs = termWeightOut.get(t)/nTotal;
      float docFreq = twtIxReader.docFreq(new Term(TweetField.TEXT.name,t));
      if(docFreq == 0){
        continue;
      }
      float pC = docFreq / twtIxReader.numDocs();
//       Change of entropy of single words
      float w = (float)((-pIs * MathUtils.log(2, pIs)) / (-pC * MathUtils.log(2, pC)));
//      //log odds
//      float w = (float)Math.log10(pIs/pC);
      termWeightOut.put(t, w);
    }
    
    //
    //
    // PriorityQueue<ScoreIxObj<List<String>>> result = new
    // PriorityQueue<ScoreIxObj<List<String>>>();
    // itemsets.count(nodeId)
    //
    // OpenObjectIntHashMap<String> queryFreq = queryTermFreq(query);
    // TransactionTree itemsets = convertResultToItemsetsInternal(rs,
    // queryFreq,
    // null, //termWeightOut,
    // itemsetsLengthOut,
    // numResults);
    //
    // if (twtCorpusLength <= 0) {
    // twtCorpusLength = 0;
    // TermEnum termEnum = twtIxReader.terms();
    // while (termEnum.next()) {
    // // close enough.. we neglect tf altogether
    // twtCorpusLength += termEnum.docFreq();
    // }
    // }
    //
    // long lenDoc = itemsetsLengthOut.longValue();
    // for (String term : termWeightOut.keys()) {
    // float termIsFreq = termWeightOut.get(term);
    // float termCorpusFreq = twtIxReader.docFreq(new Term(TweetField.TEXT.name, term));
    // if (termCorpusFreq == 0) {
    // // a repeated hashtag.. skip
    // continue;
    // }
    // // twtIxReader.terms(new Term(TweetField.TEXT.name, term)).docFreq();
    //
    // // Language model with Jelinek Mercer Smoothing considering all the returned itemsets a doc
    // // Book page 294
    // float termW = ((1 - termWeightSmoother) / termWeightSmoother)
    // * (termIsFreq / lenDoc) * (twtCorpusLength / termCorpusFreq);
    // termW = (float) ((1 + queryFreq.get(term)) * Math.log10(1 + termW));
    // // Book page 291
    //
    // // float termW = (1-termWeightSmoother) * (termIsFreq / lenDoc)
    // // + termWeightSmoother * (termCorpusFreq / twtCorpusLength);
    // // termW *= (1 + queryFreq.get(term));
    //
    // // //Language model with Dirchelet smoothing considering all the returned itemsets one
    // // document
    // // // Md(t) = Freq(t,d) + meu * Mc(t)
    // // // --------------------------
    // // // len(d) + meu
    // // float termW = (float) (1 + queryFreq.get(term)) * ((termIsFreq + (termWeightSmoother
    // // * (termCorpusFreq / twtCorpusLength)))
    // // / denim);
    // // float denim = (itemsetsLengthOut.floatValue() + termWeightSmoother);
    // // OR
    // // // Md(t) = q(t) * log (1 + f(t,d)/meu * len(C) / len(t)) - n * log(1 + l(d)/meu))
    // // // See book page 295
    // // float termW = (float) ((1 + queryFreq.get(term)) *
    // // MathUtils.log(10,
    // // 1 + ((termIsFreq / termWeightSmoother) * (twtCorpusLength / termCorpusFreq))))
    // // - subtrahend;
    // // float subtrahend = 0;
    // // IntArrayList qf = queryFreq.values();
    // // for (int i = 0; i < qf.size(); ++i) {
    // // subtrahend += qf.get(i);
    // // }
    // // subtrahend *= MathUtils.log(10, 1 + (itemsetsLengthOut.floatValue() /
    // termWeightSmoother));
    //
    // termWeightOut.put(term, termW);
    // }
    //
    // // for (Entry<Set<String>, Float> is : itemsets.entrySet()) {
    // // for (String term : is.getKey()) {
    // // float termW = termWeightOut.get(term) * is.getValue();
    // // termWeightOut.put(term, termW);
    // // }
    // // }
  }
  
  public PriorityQueue<ScoreIxObj<List<String>>> convertResultToItemsets(OpenIntFloatHashMap rs,
      String query, int numResults) throws IOException {
    
    OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
    
    TransactionTree itemsets = convertResultToItemsetsInternal(rs,
        queryTermFreq(query),
        termIds,
        null,
        numResults);
    
    List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
    termIds.keysSortedByValue(terms);
    
    PriorityQueue<ScoreIxObj<List<String>>> result = new PriorityQueue<ScoreIxObj<List<String>>>();
    Iterator<Pair<IntArrayList, Long>> itemsetIter = itemsets.iteratorClosed();
    while (itemsetIter.hasNext()) {
      Pair<IntArrayList, Long> patternPair = itemsetIter.next();
      IntArrayList pattern = patternPair.getFirst();
      int pSize = pattern.size();
      List<String> fis = Lists.newArrayListWithCapacity(pSize);
      for (int i = 0; i < pSize; ++i) {
        fis.add(terms.get(pattern.getQuick(i)));
      }
      
      result.add(new ScoreIxObj<List<String>>(fis, patternPair.getSecond().floatValue()
          / SCORE_PRECISION_MULTIPLIER));
    }
    
    return result;
  }
  
  /**
   * Side effect: removes duplicates after converting docids to actual itemsets
   * 
   * @param rs
   * @param queryTerms
   * @param queryFreq
   * @param itemsetsLengthOut
   * @return
   * @throws IOException
   */
  private TransactionTree convertResultToItemsetsInternal(OpenIntFloatHashMap rs,
      OpenObjectIntHashMap<String> queryFreq, OpenObjectIntHashMap<String> termIdOut,
      MutableLong itemsetsLengthOut, int numResults)
      throws IOException {
    
    OpenObjectFloatHashMap<Set<String>> itemsets = new OpenObjectFloatHashMap<Set<String>>();
    OpenObjectIntHashMap<Set<String>> isToDoc = new OpenObjectIntHashMap<Set<String>>();
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    int rank = 0;
    for (int i = rs.size() - 1; i >= 0; --i) {
      int hit = keyList.getQuick(i);
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          IndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      Set<String> termSet = Sets.newLinkedHashSet(Arrays.asList(terms.getTerms()));
      
      Document doc = fisIxReader.document(hit);
      float patternFreq = Float.parseFloat(doc.getFieldable(IndexBuilder.AssocField.SUPPORT.name)
          .stringValue());
      if (itemsetsLengthOut != null) {
        itemsetsLengthOut.add(patternFreq);
      }
      
      float weight;
      if (itemsets.containsKey(termSet)) {
        weight = itemsets.get(termSet);
      } else {
        // corpus level importance (added once)
        float patterIDF = (float) MathUtils.log(10, twtIxReader.numDocs() / patternFreq);
        
        float patternRank = Float.parseFloat(doc.getFieldable(IndexBuilder.AssocField.RANK.name)
            .stringValue());
        
        // (k + 1)
        // ------------------------------------- * idf * lambda
        // k * ((1-b) + b * (avgL / L)) + rank
        float delta = itemsetCorpusModelWeight * ((patterIDF * (FIS_BASE_RANK_PARAM_DEFAULT + 1))
            /
            (FIS_BASE_RANK_PARAM_DEFAULT
                * ((1 - itemSetLenWght) + (termSet.size() / itemsetLenghtAvg) * itemSetLenWght)
                + patternRank));
        weight = delta;
      }
      
      // Query level importance
      float overlap = 0;
      for (String qToken : queryFreq.keys()) {
        if (termSet.contains(qToken)) {
          overlap += queryFreq.get(qToken);
        }
      }
      
      // Sum of:
      // (k + 1)
      // ------------------------------------- * queryOverlap * (1-lambda)
      // k * ((1-b) + b * (avgL / L)) + rank
      float delta = ((overlap * /* rs.get(hit) * */(FIS_BASE_RANK_PARAM_DEFAULT + 1))
          /
          (FIS_BASE_RANK_PARAM_DEFAULT
              * ((1 - itemSetLenWght) + (termSet.size() / itemsetLenghtAvg) * itemSetLenWght)
              + rank)) * (1 - itemsetCorpusModelWeight);
      weight += delta;
      
      itemsets.put(termSet, weight);
    }
    
    TransactionTree result = new TransactionTree();
    int nextTerm = 0;
    LinkedList<Set<String>> keys = Lists.<Set<String>> newLinkedList();
    itemsets.keysSortedByValue(keys);
    Iterator<Set<String>> isIter = keys.descendingIterator();
    int r = 0;
    while (isIter.hasNext() && (numResults <= 0 || r/* result.size() */< numResults)) {
      ++r;
      Set<String> is = isIter.next();
      long itemWeight = Math.round(MathUtils.round(itemsets.get(is), 5)
          * SCORE_PRECISION_MULTIPLIER);
      
      IntArrayList patternInts = new IntArrayList(is.size());
      for (String term : is) {
        int termInt;
        if (termIdOut.containsKey(term)) {
          termInt = termIdOut.get(term);
        } else {
          termInt = nextTerm++;
          termIdOut.put(term, termInt);
        }
        patternInts.add(termInt);
      }
      
      result.addPattern(patternInts, itemWeight);
    }
    return result;
  }
  
  public Query convertResultToBooleanQuery(OpenIntFloatHashMap rs, String query, int numResults)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    BooleanQuery result = new BooleanQuery();
    PriorityQueue<ScoreIxObj<List<String>>> itemsets = convertResultToItemsets(rs,
        query,
        numResults);
    while (!itemsets.isEmpty()) {
      ScoreIxObj<List<String>> is = itemsets.poll();
      
      Query itemsetQuer = twtQparser.parse(is.obj.toString().replaceAll("[\\,\\[\\]]", ""));
      itemsetQuer.setBoost(is.score);
      result.add(itemsetQuer, Occur.SHOULD);
    }
    return result;
  }
  
  public List<Query> convertResultToQueries(OpenIntFloatHashMap rs,
      String query, int numResults)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    List<Query> result = Lists.<Query> newLinkedList();
    PriorityQueue<ScoreIxObj<List<String>>> itemsets = convertResultToItemsets(rs,
        query,
        numResults);
    while (!itemsets.isEmpty()) {
      ScoreIxObj<List<String>> is = itemsets.poll();
      
      Query itemsetQuer = twtQparser.parse(is.obj.toString().replaceAll("[\\,\\[\\]]", ""));
      itemsetQuer.setBoost(is.score);
      result.add(itemsetQuer);
    }
    return result;
  }
  
  public void close() throws IOException {
    if (fisSearcher != null)
      fisSearcher.close();
    if (fisIxReader != null)
      fisIxReader.close();
    if (twtSearcher != null)
      twtSearcher.close();
    if (twtIxReader != null)
      twtIxReader.close();
  }
  
  private int addQualifiedResults(TopDocs rs, OpenIntFloatHashMap result,
      Set<String> queryTerms, Set<ScoreIxObj<String>> extraTerms, float minScore, float fusionK)
      throws IOException {
    int levelHits = 0;
    int rank = 0;
    for (ScoreDoc scoreDoc : rs.scoreDocs) {
      if (scoreDoc.score < minScore) {
        LOG.debug("Because of low score, pruning after result number {} out of {}",
            rank,
            rs.scoreDocs.length);
        break;
      }
      ++rank;
      
      if (!result.containsKey(scoreDoc.doc)) {
        ++levelHits;
      }
      
      float fusion = result.get(scoreDoc.doc);
      fusion += 1.0f / (fusionK + rank);
      result.put(scoreDoc.doc, fusion);
      
      TermFreqVector termVector = fisIxReader.getTermFreqVector(scoreDoc.doc,
          IndexBuilder.AssocField.ITEMSET.name);
      for (String term : termVector.getTerms()) {
        if (queryTerms.contains(term)) {
          continue;
        }
        // scoreDoc.score or fusion didn't change performance in a visible way
        extraTerms.add(new ScoreIxObj<String>(term, fusion));
      }
    }
    
    return levelHits;
  }
  
  private SetView<ScoreIxObj<String>> expandRecursive(Set<String> queryTerms,
      Set<ScoreIxObj<String>> extraTerms,
      Set<ScoreIxObj<String>> doneTerms,
      OpenIntFloatHashMap resultSet, int levelNumHits, float levelMinScore, float levelRankingParam)
      throws org.apache.lucene.queryParser.ParseException,
      IOException {
    
    Set<ScoreIxObj<String>> extraTerms2 = Sets.<ScoreIxObj<String>> newHashSet();
    
    // // Perform only one query per qTerm, written as simple as possible
    // for (String qterm : queryTerms) {
    // BooleanQuery query = new BooleanQuery(true);
    //
    // // Must match at least one of the xterms beside the qterm
    // query.setMinimumNumberShouldMatch(1);
    //
    // Query qtermQuery = fisQparser.parse(qterm);
    // // qtermQuery.setBoost(similarity.idf(ixReader.docFreq(new
    // // Term(IndexBuilder.AssocField.ITEMSET.name, qterm)), ixReader.numDocs()));
    //
    // query.add(qtermQuery, Occur.MUST);
    //
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    //
    // Query xtermQuery = fisQparser.parse(xterm.toString());
    // // xtermQuery.setBoost(xterm.score); // * extraTerms.size());
    //
    // query.add(xtermQuery, Occur.SHOULD);
    // }
    //
    // // // Does this have any effect at all?
    // // query.setBoost(boost);
    //
    // TopDocs rs = fisSearcher.search(query, fisNumHits);
    //
    // int levelHits = addQualifiedResults(rs, resultSet, queryTerms, extraTerms2, levelMinScore,
    // levelRankingParam);
    // LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    // }
    
    // Only one query (a long one), of pairs of query and extra
    // Seems to be getting high precision pairs, and performs best
    BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    for (String qterm : queryTerms) {
      for (ScoreIxObj<String> xterm : extraTerms) {
        assert !doneTerms.contains(xterm);
        
        Query xtermQuery = fisQparser.parse(qterm + " " + xterm.obj);
        xtermQuery.setBoost(xterm.score);
        // Scoring the term not the query "+ xterm.toString()" causes an early topic drift
        
        query.add(xtermQuery, Occur.SHOULD);
      }
    }
    // // Does this have any effect at all?
    // query.setBoost(boost);
    
    TopDocs rs = fisSearcher.search(query, fisNumHits);
    
    int levelHits = addQualifiedResults(rs,
        resultSet,
        queryTerms,
        extraTerms2,
        levelMinScore,
        levelRankingParam);
    LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    // This is the proof of concept that hits the index with as many queries as terms
    // for (String qterm : queryTerms) {
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    //
    // Query xtermQuery = fisQparser.parse(qterm + " " + xterm.toString());
    //
    // TopDocs rs = fisSearcher.search(xtermQuery, levelNumHits);
    //
    // int levelHits = addQualifiedResults(rs,
    // resultSet,
    // queryTerms,
    // extraTerms2,
    // levelMinScore,
    // levelRankingParam);
    // LOG.debug("Added {} results from the query {}", levelHits, xtermQuery.toString());
    // }
    // }
    
    doneTerms.addAll(extraTerms);
    return Sets.difference(extraTerms2, doneTerms);
    
  }
  
  private OpenObjectIntHashMap<String> queryTermFreq(String query) throws IOException {
    OpenObjectIntHashMap<String> queryFreq = new OpenObjectIntHashMap<String>();
    // String[] queryTokens = query.toString().split("\\W");
    TokenStream queryTokens = ANALYZER.tokenStream(TweetField.TEXT.name,
        new StringReader(query.toString()));
    queryTokens.reset();
    
    // Set<Term> queryTerms = Sets.newHashSet();
    // parsedQuery.extractTerms(queryTerms);
    while (queryTokens.incrementToken()) {
      CharTermAttribute attr = (CharTermAttribute) queryTokens.getAttribute(queryTokens
          .getAttributeClassesIterator().next());
      String token = attr.toString();
      
      int freq = queryFreq.get(token);
      queryFreq.put(token, ++freq);
    }
    return queryFreq;
  }
  
}
