package ca.uwaterloo.twitter.queryexpand;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
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

import ca.uwaterloo.twitter.ItemSetIndexBuilder;
import ca.uwaterloo.twitter.ItemSetSimilarity;
import ca.uwaterloo.twitter.TwitterAnalyzer;
import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;
import ca.uwaterloo.twitter.TwitterSimilarity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.ibm.icu.text.SimpleDateFormat;

public class FISQueryExpander {
  
  private static Logger LOG = LoggerFactory.getLogger(FISQueryExpander.class);
  
  private static final String FIS_INDEX_OPTION = "fis_inc_index";
  private static final String TWT_INC_INDEX_OPTION = "twt_inc_index";
  private static final String TWT_CHUNK_INDEX_OPTION = "twt_chunk_index";
  
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
  
  private static final Analyzer ANALYZER = new TwitterAnalyzer();// new
                                                                 // EnglishAnalyzer(Version.LUCENE_36);
  private static final boolean CHAR_BY_CHAR = false;
  
  private static final int MIN_ITEMSET_SIZE = 1;
  
  private static final String RETWEET_QUERY = "RT";
  
  private static final float ITEMSET_LEN_AVG_DEFAULT = 5;
  
  private static final float ITEMSET_LEN_WEIGHT_DEFAULT = 0.33f;
  
  private static final float ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT = 0.77f;
  private static final float TWITTER_CORPUS_MODEL_WEIGHT_DEFAULT = 0.33f;
  
  private static final float TERM_WEIGHT_SMOOTHER_DEFAULT = 10000.0f;
  
  private static final float SCORE_PRECISION_MULTIPLIER = 100000.0f;
  
  private static final TermWeigting TERM_WEIGHTIN_DEFAULT = TermWeigting.PROB_QUERY;
  public static final boolean PROPAGATE_IS_WEIGHTS_DEFAULT = true;
  
  private static final String COLLECTION_STRING_CLEANER = "[\\,\\[\\]]";
  
  private static final boolean QUERY_SUBSET_BOOST_IDF = false;
  
  public static enum TermWeigting {
    PROB_QUERY("pq"),
    KL_DIVERG("kl");
    
    public final String name;
    
    TermWeigting(String s) {
      name = s;
    }
  };
  
  /**
   * @param args
   * @throws IOException
   * @throws org.apache.lucene.queryParser.ParseException
   * @throws java.text.ParseException
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException,
      org.apache.lucene.queryParser.ParseException, java.text.ParseException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("frequent itemsets index location").create(FIS_INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("twitter incremental index location").create(TWT_INC_INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("twitter chunked indexes root").create(TWT_CHUNK_INDEX_OPTION));
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
    
    if (!(cmdline.hasOption(FIS_INDEX_OPTION) && cmdline.hasOption(TWT_INC_INDEX_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(FISQueryExpander.class.getName(), options);
      System.exit(-1);
    }
    
    File fisIndexLocation = new File(cmdline.getOptionValue(FIS_INDEX_OPTION));
    if (!fisIndexLocation.exists()) {
      System.err.println("Error: " + fisIndexLocation + " does not exist!");
      System.exit(-1);
    }
    
    File twtIncIxLocation = new File(cmdline.getOptionValue(TWT_INC_INDEX_OPTION));
    if (!twtIncIxLocation.exists()) {
      System.err.println("Error: " + twtIncIxLocation + " does not exist!");
      System.exit(-1);
    }
    
    File twtChnkIxRoot = new File(cmdline.getOptionValue(TWT_CHUNK_INDEX_OPTION));
    if (!twtChnkIxRoot.exists()) {
      System.err.println("Error: " + twtChnkIxRoot + " does not exist!");
      System.exit(-1);
    }
    
    File[] twtChnkIxLocs = twtChnkIxRoot.listFiles();
    Arrays.sort(twtChnkIxLocs);
    
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
    
    TermWeigting termWeighting = TERM_WEIGHTIN_DEFAULT;
    // TODO: from commandline
    
    boolean propagateISWeights = PROPAGATE_IS_WEIGHTS_DEFAULT;
    // TODO: from commandline
    
    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    FISQueryExpander qEx = null;
    try {
      qEx = new FISQueryExpander(fisIndexLocation, twtIncIxLocation, null, null);
      StringBuilder query = new StringBuilder();
      do {
        out.print(">"); 
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
        } else if (cmd.equals("q:")) {
          mode = 6; // terms with query probability
          termWeighting = TermWeigting.PROB_QUERY;
        } else if (cmd.equals("d:")) {
          mode = 7; // terms with query divergence
          termWeighting = TermWeigting.KL_DIVERG;
        } else if (cmd.equals("r:")) {
          mode = 10; // results
        } else if (cmd.equals("e:")) {
          mode = 20; // expanded
        } else if (cmd.equals("t:")) {
          mode = 30; // set the time
        } else {
          out.println("Must prefix either\n i: for itemsets \n r: for results \n e: for expanded results \n t|d|q: for terms");
          continue;
        }
        
        query.delete(0, 2);
        
        if (mode == 30) {
          try {
            if (qEx != null) {
              qEx.close();
            }
            qEx = new FISQueryExpander(fisIndexLocation, twtIncIxLocation, twtChnkIxLocs,
                query.toString().trim());
            out.println("Queries will be executed as if the time is: "
                + qEx.getTimeFormatted());
          } catch (java.text.ParseException e) {
            System.err.println("Cannot set the time: " + e.getMessage());
          }
          continue;
        }
        
        out.println();
        out.println(query.toString());
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
        } else if (mode == 5 || mode == 6 || mode == 7) {
          
          PriorityQueue<ScoreIxObj<String>> weightedTerms = null;
          if (TermWeigting.PROB_QUERY.equals(termWeighting)) {
            weightedTerms = qEx
                .convertResultToWeightedTermsConditionalProb(fisRs,
                    query.toString(),
                    -1, propagateISWeights);
          } else if (TermWeigting.KL_DIVERG.equals(termWeighting)) {
            weightedTerms = qEx
                .convertResultToWeightedTermsKLDivergence(fisRs,
                    query.toString(),
                    -1, propagateISWeights);
          }
          
          int i = 0;
          while (!weightedTerms.isEmpty()) {
            ScoreIxObj<String> t = weightedTerms.poll();
            out.println(++i + " (" + t.score + "): " + t.obj);
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
            Fieldable created = hit.getFieldable(TweetField.TIMESTAMP.name);
            // out.println();
            out.println(String.format("%4d (%.4f): %s\t%s\t%s\t%s",
                ++t,
                scoreDoc.score,
                hit.getFieldable(TweetField.TEXT.name).stringValue(),
                hit.getFieldable(TweetField.SCREEN_NAME.name).stringValue(),
                (created == null ? "" : created.stringValue()),
                hit.getFieldable(TweetField.ID.name).stringValue()));
            
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
  
  private final String timeFormatted;
  
  private String getTimeFormatted() {
    return timeFormatted;
  }
  
  private final QueryParser fisQparser;
  private final IndexSearcher fisSearcher;
  private final IndexReader fisIxReader;
  
  private final int fisNumHits = NUM_HITS_INTERNAL_DEFAULT;
  private final float fisBaseRankingParam = FIS_BASE_RANK_PARAM_DEFAULT;
  
  private final Similarity fisSimilarity;
  
  private final QueryParser twtQparser;
  private final IndexSearcher twtSearcher;
  private final MultiReader twtIxReader;
  
  // private final int twtNumHits = NUM_HITS_DEFAULT;
  
  private final Similarity twtSimilarity;
  
  private float itemsetLenghtAvg = ITEMSET_LEN_AVG_DEFAULT;
  
  // As in Lambda of Jelink Mercer smoothing
  private float itemsetCorpusModelWeight = ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT;
  private float twitterCorpusModelWeight = TWITTER_CORPUS_MODEL_WEIGHT_DEFAULT;
  
  // As in Mue of Dirchilet smoothing
  private float termWeightSmoother = TERM_WEIGHT_SMOOTHER_DEFAULT;
  
  private float itemSetLenWght = ITEMSET_LEN_WEIGHT_DEFAULT;
  
  private final long queryTime;
  
  // TODO command line
  private static boolean incremental = true;
  
  /**
   * 
   * @param fisIndexLocation
   * @param twtIndexLocation
   * @param timeFormatted
   *          example: Sun Feb 06 10:38:43 +0000 2011
   * @throws IOException
   * @throws java.text.ParseException
   */
  public FISQueryExpander(File fisIncIxLocation,
      File twtIncIxLoc, File[] twtChunkIxLocs, String timeFormatted)
      throws IOException, java.text.ParseException {
    SimpleDateFormat dFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
    if (timeFormatted != null) {
      this.timeFormatted = timeFormatted;
      this.queryTime = dFmt.parse(timeFormatted)
          .getTime();
    } else {
      this.queryTime = System.currentTimeMillis();
      this.timeFormatted = dFmt.format(new Date(queryTime));
    }
    
    Directory fisdir = new MMapDirectory(fisIncIxLocation);
    fisIxReader = IndexReader.open(fisdir);
    fisSearcher = new IndexSearcher(fisIxReader);
    fisSimilarity = new ItemSetSimilarity();
    fisSearcher.setSimilarity(fisSimilarity);
    
    fisQparser = new QueryParser(Version.LUCENE_36,
        ItemSetIndexBuilder.AssocField.ITEMSET.name,
        ANALYZER);
    fisQparser.setDefaultOperator(Operator.AND);
    
    List<IndexReader> ixRds = Lists.newLinkedList();
    long incrEndTime = openTweetIndexesBeforeQueryTime(twtIncIxLoc,
        true,
        false,
        Long.MIN_VALUE,
        ixRds);
    if (twtChunkIxLocs != null) {
      int i = 0;
      long prevChunkEndTime = incrEndTime;
      while (i < twtChunkIxLocs.length - 1) {
        prevChunkEndTime = openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i++],
            false,
            false,
            prevChunkEndTime,
            ixRds);
      }
      openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i], false, true, prevChunkEndTime, ixRds);
    }
    twtIxReader = new MultiReader(ixRds.toArray(new IndexReader[0]));
    twtSearcher = new IndexSearcher(twtIxReader);
    twtSimilarity = new TwitterSimilarity();
    twtSearcher.setSimilarity(twtSimilarity);
    
    twtQparser = new QueryParser(Version.LUCENE_36, TweetField.TEXT.name, ANALYZER);
    twtQparser.setDefaultOperator(Operator.AND);
    
    BooleanQuery.setMaxClauseCount(fisNumHits * fisNumHits);
  }
  
  private long openTweetIndexesBeforeQueryTime(File twtIndexLocation, boolean pIncremental,
      boolean exceedTime, long windowStart, List<IndexReader> ixReadersOut) throws IOException {
    assert !(pIncremental && exceedTime) : "Those are mutually exclusive modes";
    long result = -1;
    File[] startFolders = twtIndexLocation.listFiles();
    Arrays.sort(startFolders);
    int minIx = -1;
    int maxIx = -1;
    for (int i = 0; i < startFolders.length; ++i) {
      long folderStartTime = Long.parseLong(startFolders[i].getName());
      if (minIx == -1 && folderStartTime >= windowStart) {
        minIx = i;
      }
      if (folderStartTime < queryTime) {
        maxIx = i;
      } else {
        break;
      }
    }
//    if (minIx == maxIx) {
//      startFolders = new File[] { startFolders[minIx] };
//    } else {
//      startFolders = Arrays.copyOfRange(startFolders, minIx, maxIx);
//    }
    startFolders = Arrays.copyOfRange(startFolders, minIx, maxIx+1);
    for (File startFolder : startFolders) {
      boolean lastOne = false;
      File incrementalFolder = null;
      File[] endFolderArr = startFolder.listFiles();
      Arrays.sort(endFolderArr);
      for (File endFolder : endFolderArr) {
        if (Long.parseLong(endFolder.getName()) > queryTime) {
          if (pIncremental) {
            break;
          } else {
            if (exceedTime) {
              lastOne = true;
            } else {
              break;
            }
          }
        }
        if (pIncremental) {
          incrementalFolder = endFolder;
        } else {
          Directory twtdir = new MMapDirectory(endFolder);
          ixReadersOut.add(IndexReader.open(twtdir));
          result = Long.parseLong(endFolder.getName());
        }
        if (lastOne) {
          assert !pIncremental;
          break;
        }
      }
      if (incrementalFolder != null) {
        assert pIncremental;
        assert startFolders.length == 1;
        Directory twtdir = new MMapDirectory(incrementalFolder);
        ixReadersOut.add(IndexReader.open(twtdir));
        result = Long.parseLong(incrementalFolder.getName());
        break; // shouldn't be needed
      }
    }
    return result;
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
    
    MutableLong qLen = new MutableLong(0);
    OpenObjectFloatHashMap<String> queryTermWeight = queryTermFreq(queryStr, qLen);
    
    float totalIDF = 0;
    if (QUERY_SUBSET_BOOST_IDF) {
      for (String qt : queryTermWeight.keys()) {
        float idf = twtSimilarity.idf(twtIxReader.docFreq(new Term(TweetField.TEXT.name, qt)),
            twtIxReader.numDocs());
        idf *= queryTermWeight.get(qt);
        totalIDF += idf;
        queryTermWeight.put(qt, idf);
      }
    }
    BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    Set<String> querySet = Sets.newCopyOnWriteArraySet(queryTermWeight.keys());
    if (querySet.size() > 2) {
      for (Set<String> querySubSet : Sets.powerSet(querySet)) {
        if (querySubSet.size() < 2) {
          continue;
        }
        Query subQuery = fisQparser.parse(querySubSet.toString()
            .replaceAll(COLLECTION_STRING_CLEANER, ""));
        
        float querySubSetW = 0;
        for (String qTerm : querySubSet) {
          querySubSetW += queryTermWeight.get(qTerm);
        }
        
        if (QUERY_SUBSET_BOOST_IDF) {
          subQuery.setBoost(querySubSetW / totalIDF);
        } else {
          subQuery.setBoost(querySubSetW / qLen.floatValue());
        }
        
        query.add(subQuery, Occur.SHOULD);
      }
    } else {
      Query subQuery = fisQparser.parse(querySet.toString().replaceAll(COLLECTION_STRING_CLEANER,
          ""));
      subQuery.setBoost(1);
      query.add(subQuery, Occur.SHOULD);
    }
    
    // Query allQuery = fisQparser.parse(queryStr);
    // // allQuery.setBoost(baseBoost);
    // allQuery = allQuery.rewrite(fisIxReader);
    //
    // Set<Term> queryTermsTemp = Sets.<Term> newHashSet();
    // Set<String> queryTerms = Sets.<String> newHashSet();
    // allQuery.extractTerms(queryTermsTemp);
    // for (Term term : queryTermsTemp) {
    // queryTerms.add(term.text());
    // }
    // queryTermsTemp = null;
    //
    // // This boost should be used only for the allQuery
    // // query.setBoost(baseBoost);
    // query.add(allQuery, Occur.SHOULD);
    //
    // if (queryTerms.size() > 2) {
    // String[] queryArr = queryTerms.toArray(new String[0]);
    // for (int i = 0; i < queryTerms.size() - 1; ++i) {
    // for (int j = i + 1; j < queryTerms.size(); ++j) {
    // Query pairQuery = fisQparser.parse(queryArr[i] + " " + queryArr[j]);
    // pairQuery.setBoost((2.0f / queryTerms.size())); // * baseBoost);
    // query.add(pairQuery, Occur.SHOULD);
    // }
    // }
    // queryArr = null;
    // }
    
    OpenIntFloatHashMap resultSet = new OpenIntFloatHashMap();
    TopDocs rs = fisSearcher.search(query, fisNumHits);
    
    Set<ScoreIxObj<String>> extraTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int levelHits = addQualifiedResults(rs,
        resultSet,
        querySet,
        extraTerms,
        minScore,
        fisBaseRankingParam);
    LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    Set<ScoreIxObj<String>> doneTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int level = 1;
    while (extraTerms.size() > 0 && level < MAX_LEVELS_EXPANSION) {
      float fusionK = (float) (fisBaseRankingParam + Math.pow(10, level));
      extraTerms = expandRecursive(queryTermWeight,
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
  
  public PriorityQueue<ScoreIxObj<String>> convertResultToWeightedTermsKLDivergence(
      OpenIntFloatHashMap rs,
      String query, int numItemsetsToConsider, boolean propagateISWeight) throws IOException {
    PriorityQueue<ScoreIxObj<String>> result = new PriorityQueue<ScoreIxObj<String>>();
    
    OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
    OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query, null);
    TransactionTree itemsets = convertResultToItemsetsInternal(rs,
        queryFreq,
        termIds,
        numItemsetsToConsider);
    
    if (itemsets.isTreeEmpty()) {
      return result;
    }
    
    List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
    termIds.keysSortedByValue(terms);
    
    OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>(terms.size());
    float totalW = 0;
    
    Iterator<Pair<IntArrayList, Long>> itemsetIter = itemsets.iteratorClosed();
    while (itemsetIter.hasNext()) {
      Pair<IntArrayList, Long> patternPair = itemsetIter.next();
      
      float w = propagateISWeight ?
          (patternPair.getSecond().floatValue()
          / SCORE_PRECISION_MULTIPLIER) :
          1;
      totalW += w;
      
      IntArrayList pattern = patternPair.getFirst();
      int pSize = pattern.size();
      for (int i = 0; i < pSize; ++i) {
        String ft = terms.get(pattern.getQuick(i));
        if (queryFreq.containsKey(ft)) {
          continue;
        }
        termFreq.put(ft, termFreq.get(ft) + w);
      }
    }
    
    for (String t : terms) {
      if (queryFreq.containsKey(t)) {
        continue;
      }
      // Collection metric
      Term tTerm = new Term(TweetField.TEXT.name, t);
      float docFreqC = twtIxReader.docFreq(tTerm);
      if (docFreqC == 0) {
        continue;
      }
      float pCollection = docFreqC / twtIxReader.numDocs();
      
      // KL Divergence considering all the itemsets as a document
      
      float pDoc = termFreq.get(t) / totalW;
      float score = pDoc * (float) MathUtils.log(2, pDoc / pCollection);
      result.add(new ScoreIxObj<String>(t, score));
    }
    
    return result;
  }
  
  public PriorityQueue<ScoreIxObj<String>> convertResultToWeightedTermsConditionalProb(
      OpenIntFloatHashMap rs,
      String query, int numItemsetsToConsider, boolean propagateISWeight) throws IOException {
    
    PriorityQueue<ScoreIxObj<String>> result = new PriorityQueue<ScoreIxObj<String>>();
    
    OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
    
    OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query, null);
    TransactionTree itemsets = convertResultToItemsetsInternal(rs,
        queryFreq,
        termIds,
        numItemsetsToConsider);
    
    if (itemsets.isTreeEmpty()) {
      return result;
    }
    
    List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
    termIds.keysSortedByValue(terms);
    
    Set<String> querySet = Sets.newCopyOnWriteArraySet(queryFreq.keys());
    Set<Set<String>> queryPowerSet = Sets.powerSet(querySet);
    
    int capacity = queryPowerSet.size() * (terms.size() - queryFreq.size());
    OpenObjectFloatHashMap<Set<String>> subsetFreq = new OpenObjectFloatHashMap<Set<String>>(
        Math.max(0, capacity));
    OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>(terms.size());
    float totalW = 0;
    
    Iterator<Pair<IntArrayList, Long>> itemsetIter = itemsets.iteratorClosed();
    while (itemsetIter.hasNext()) {
      Pair<IntArrayList, Long> patternPair = itemsetIter.next();
      
      float w = propagateISWeight ?
          (patternPair.getSecond().floatValue()
          / SCORE_PRECISION_MULTIPLIER) :
          1;
      totalW += w;
      
      IntArrayList pattern = patternPair.getFirst();
      int pSize = pattern.size();
      Set<String> fisTerms = Sets.newHashSet();
      for (int i = 0; i < pSize; ++i) {
        fisTerms.add(terms.get(pattern.getQuick(i)));
      }
      
      boolean weightNotAdded = true;
      for (String ft : fisTerms) {
        if (queryFreq.containsKey(ft)) {
          continue;
        }
        termFreq.put(ft, termFreq.get(ft) + w);
        for (Set<String> qSub : queryPowerSet) {
          if (qSub.isEmpty()) {
            continue;
          }
          if (!Sets.intersection(fisTerms, qSub).equals(qSub)) {
            // This query s
            continue;
          } else if (weightNotAdded) {
            subsetFreq.put(qSub, subsetFreq.get(qSub) + w);
          }
          
          Set<String> key = Sets.union(qSub, ImmutableSet.<String> of(ft));
          subsetFreq.put(key, subsetFreq.get(key) + w);
        }
        weightNotAdded = false;
      }
    }
    
    subsetFreq.put(ImmutableSet.<String> of(), totalW);
    for (String t : terms) {
      if (queryFreq.containsKey(t)) {
        continue;
      }
      // Collection metric
      Term tTerm = new Term(TweetField.TEXT.name, t);
      float docFreqC = twtIxReader.docFreq(tTerm);
      if (docFreqC == 0) {
        continue;
      }
      
      // odds of the term (not log odds)
      float termCorpusQuality = (termWeightSmoother + docFreqC) / twtIxReader.numDocs();
      termCorpusQuality = termCorpusQuality / (1 - termCorpusQuality);
      termCorpusQuality = (float) Math.log(termCorpusQuality);
      
      // IDF is has very large scale compared to probabilities
      // float termCorpusQuality = (float) (Math.log(twtIxReader.numDocs() / (float) (docFreqC +
      // 1)) + 1.0);
      
      // Query metric
      Set<String> tAsSet = ImmutableSet.<String> of(t);
      subsetFreq.put(tAsSet, termFreq.get(t));
      
      LinkedHashMap<Set<String>, Float> termQueryQuality = Maps
          .<Set<String>, Float> newLinkedHashMap();
      
      for (Set<String> qSub : queryPowerSet) {
        
        float freqSub = subsetFreq.get(qSub);
        
        if (freqSub == 0) {
          continue;
        }
        
        Set<String> qSubExp = Sets.union(qSub, tAsSet);
        float jointFreq = subsetFreq.get(qSubExp);
        
        // //Mutual information No normalization:
        // if (jf != 0) {
        // float jp = jf / totalW;
        // numer += jp * (Math.log(jf / (ft1 * ft2)) + lnTotalW);
        // denim += jp * Math.log(jp);
        // }
        float pExp = jointFreq / freqSub;
        
        termQueryQuality.put(qSubExp, pExp);
      }
      
      float termQueryQualityAggr = 0;
      for (String qTerm : querySet) {
        // for (List<String> qtPair : Sets.cartesianProduct(tAsSet, querySet)) {
        float qTermReps = queryFreq.get(qTerm);
        List<String> qtPair = ImmutableList.<String> of(t, qTerm);
        termQueryQualityAggr += qTermReps * aggregateTermQueryQuality(termQueryQuality,
            Sets.newCopyOnWriteArraySet(qtPair),
            termQueryQuality.get(tAsSet));
      }
      if (termQueryQualityAggr >= 1) {
        // FIXME: This happens in case of repeated hashtag
        termQueryQualityAggr = (float) (1 - 1E-6);
      }
      
      termQueryQualityAggr /= (1 - termQueryQualityAggr);
      termQueryQualityAggr = (float) Math.log(termQueryQualityAggr);
      
      float score = (float) (twitterCorpusModelWeight * termCorpusQuality +
          (1 - twitterCorpusModelWeight) * termQueryQualityAggr);
      result.add(new ScoreIxObj<String>(t, score));
    }
    
    return result;
    
  }
  
  private float aggregateTermQueryQuality(LinkedHashMap<Set<String>, Float> termQueryQuality,
      Set<String> subset, float currP) {
    Iterator<Set<String>> iter = termQueryQuality.keySet().iterator();
    Set<String> key = null;
    while (iter.hasNext()) {
      key = iter.next();
      if (subset.isEmpty() || key.containsAll(subset)) {
        break;
      } else {
        key = null;
      }
    }
    
    float result = currP;
    if (key != null) {
      result *= termQueryQuality.remove(key);
      if (result != 0) {
        result += aggregateTermQueryQuality(termQueryQuality, key, result);
      }
    }
    return result;
  }
  
  public PriorityQueue<ScoreIxObj<List<String>>> convertResultToItemsets(OpenIntFloatHashMap rs,
      String query, int numResults) throws IOException {
    
    PriorityQueue<ScoreIxObj<List<String>>> result = new PriorityQueue<ScoreIxObj<List<String>>>();
    
    OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
    
    TransactionTree itemsets = convertResultToItemsetsInternal(rs,
        queryTermFreq(query, null),
        termIds,
        numResults);
    
    if (itemsets.isTreeEmpty()) {
      return result;
    }
    
    List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
    termIds.keysSortedByValue(terms);
    
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
      OpenObjectFloatHashMap<String> queryFreq, OpenObjectIntHashMap<String> termIdOut,
      int numResults)
      throws IOException {
    
    OpenObjectFloatHashMap<Set<String>> itemsets = new OpenObjectFloatHashMap<Set<String>>();
    // OpenObjectIntHashMap<Set<String>> isToDoc = new OpenObjectIntHashMap<Set<String>>();
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    int rank = 0;
    for (int i = rs.size() - 1; i >= 0; --i) {
      int hit = keyList.getQuick(i);
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          ItemSetIndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      Set<String> termSet = Sets.newCopyOnWriteArraySet(Arrays.asList(terms.getTerms()));
      
      float weight;
      if (itemsets.containsKey(termSet)) {
        weight = itemsets.get(termSet);
      } else {
        // corpus level importance (added once)
        Document doc = fisIxReader.document(hit);
        float patternFreq = Float.parseFloat(doc
            .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
            .stringValue());
        float patterIDF = (float) MathUtils.log(10, twtIxReader.numDocs() / patternFreq);
        
        float patternRank = Float.parseFloat(doc
            .getFieldable(ItemSetIndexBuilder.AssocField.RANK.name)
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
      
      Query itemsetQuer = twtQparser.parse(is.obj.toString().replaceAll(COLLECTION_STRING_CLEANER,
          ""));
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
      
      Query itemsetQuer = twtQparser.parse(is.obj.toString().replaceAll(COLLECTION_STRING_CLEANER,
          ""));
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
          ItemSetIndexBuilder.AssocField.ITEMSET.name);
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
  
  private SetView<ScoreIxObj<String>> expandRecursive(
      OpenObjectFloatHashMap<String> queryTermWeight,
      Set<ScoreIxObj<String>> extraTerms,
      Set<ScoreIxObj<String>> doneTerms,
      OpenIntFloatHashMap resultSet, int levelNumHits, float levelMinScore, float levelRankingParam)
      throws org.apache.lucene.queryParser.ParseException,
      IOException {
    
    Set<ScoreIxObj<String>> extraTerms2 = Sets.<ScoreIxObj<String>> newHashSet();
    
    // This seems to suffer from the bad scoring of low overlap query
    // // +(q1 OR q2 OR q..) AND ONE_OF(xtra1 OR xtra2 OR ...)
    // // One query, and the first portion will limit the corpus
    // BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    // query.setMinimumNumberShouldMatch(1);
    // fisQparser.setDefaultOperator(Operator.OR);
    // StringBuilder qtermsQueryStr = new StringBuilder();
    // for (String qterm : queryTerms) {
    // qtermsQueryStr.append(qterm).append(" ");
    // }
    // Query qtermsQuery = fisQparser.parse(qtermsQueryStr.toString());
    // query.add(new BooleanClause(qtermsQuery, Occur.MUST));
    //
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    //
    // Query xtermQuery = fisQparser.parse(xterm.obj);
    // xtermQuery.setBoost(xterm.score);
    // // Scoring the term not the query "+ xterm.toString()" causes an early topic drift
    //
    // query.add(xtermQuery, Occur.SHOULD);
    // }
    // // // Does this have any effect at all?
    // // query.setBoost(boost);
    // fisQparser.setDefaultOperator(Operator.AND);
    // TopDocs rs = fisSearcher.search(query, fisNumHits);
    //
    // int levelHits = addQualifiedResults(rs,
    // resultSet,
    // queryTerms,
    // extraTerms2,
    // levelMinScore,
    // levelRankingParam);
    // LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    // // Perform only one query per qTerm, written as simple as possible
    // for (String qterm : queryTerms) {
    // BooleanQuery query = new BooleanQuery(true);
    //
    // // Must match at least one of the xterms beside the qterm
    // query.setMinimumNumberShouldMatch(1);
    //
    // Query qtermQuery = fisQparser.parse(qterm);
    // // qtermQuery.setBoost(similarity.idf(ixReader.docFreq(new
    // // Term( ItemSetIndexBuilder.AssocField.ITEMSET.name, qterm)), ixReader.numDocs()));
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
    for (String qterm : queryTermWeight.keys()) {
      for (ScoreIxObj<String> xterm : extraTerms) {
        assert !doneTerms.contains(xterm);
        
        float xtermWeight;
        if (QUERY_SUBSET_BOOST_IDF) {
          xtermWeight = twtSimilarity.idf(twtIxReader.docFreq(new Term(TweetField.TEXT.name,
              xterm.obj)), twtIxReader.numDocs());
        } else {
          xtermWeight = xterm.score;
        }
        
        Query xtermQuery = fisQparser.parse(qterm + "^" + queryTermWeight.get(qterm) + " "
            + xterm.obj);
        xtermQuery.setBoost(xtermWeight);
        // Scoring the term not the query "+ xterm.toString()" causes an early topic drift
        
        query.add(xtermQuery, Occur.SHOULD);
      }
    }
    // // Does this have any effect at all?
    // query.setBoost(boost);
    
    // This is also not needed, the query is already so simple
    // query = (BooleanQuery) query.rewrite(fisIxReader);
    
    TopDocs rs = fisSearcher.search(query, fisNumHits);
    
    int levelHits = addQualifiedResults(rs,
        resultSet,
        Sets.newCopyOnWriteArraySet(queryTermWeight.keys()),
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
  
  private OpenObjectFloatHashMap<String> queryTermFreq(String query, MutableLong qLenOut)
      throws IOException {
    OpenObjectFloatHashMap<String> queryFreq = new OpenObjectFloatHashMap<String>();
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
      
      float freq = queryFreq.get(token);
      queryFreq.put(token, ++freq);
      
      if (qLenOut != null) {
        qLenOut.add(1);
      }
    }
    return queryFreq;
  }
  
}
