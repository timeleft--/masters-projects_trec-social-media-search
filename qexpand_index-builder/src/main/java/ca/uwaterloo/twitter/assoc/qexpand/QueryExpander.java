package ca.uwaterloo.twitter.assoc.qexpand;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
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
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.list.ObjectArrayList;
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
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
          LinkedHashMap<Set<String>, Float> itemsets = qEx.convertResultToItemsets(fisRs,
              query.toString(),
              -1);
          // NUM_HITS_DEFAULT);
          
          int i = 0;
          for (Entry<Set<String>, Float> hit : itemsets.entrySet()) {
            out.println(++i + " (" + hit.getValue() + "): " + hit.getKey().toString());
          }
        } else if (mode == 5) {
          OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>();
          MutableLong itemsetsLength = new MutableLong();
          
          qEx.converResultToWeightedTerms(fisRs, query.toString(), termFreq, itemsetsLength, -1);
          
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
  
  // As in Jelink Mercer smoothing
  private float itemsetCorpusModelWeight = ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT;;
  
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
  
  public void converResultToWeightedTerms(OpenIntFloatHashMap rs,
      String query, OpenObjectFloatHashMap<String> termFreqOut,
      MutableLong itemsetsLengthOut, int numResults) throws IOException {
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    for (int i = rs.size() - 1; i >= 0 && (numResults <= 0 || termFreqOut.size() < numResults); --i) {
      int hit = keyList.getQuick(i);
      
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          IndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      
      for (String term : terms.getTerms()) {
        termFreqOut.put(term, termFreqOut.get(term) + 1);
      }
      
      if (itemsetsLengthOut != null) {
        itemsetsLengthOut.add(terms.size());
      }
      
    }
  }
  
  public LinkedHashMap<Set<String>, Float> convertResultToItemsets(OpenIntFloatHashMap rs,
      String query, int numResults) throws IOException {
    
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
    
    return convertResultToItemsetsInternal(rs, queryFreq, numResults);
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
  private LinkedHashMap<Set<String>, Float> convertResultToItemsetsInternal(OpenIntFloatHashMap rs,
      OpenObjectIntHashMap<String> queryFreq, int numResults)
      throws IOException {
    
    LinkedHashMap<Set<String>, Float> result = Maps.<Set<String>, Float> newLinkedHashMap();
    
    float lenWght = ITEMSET_LEN_WEIGHT_DEFAULT;
    // for (String qToken : queryFreq.keys()) {
    // lenWght += queryFreq.get(qToken);
    // }
    // lenWght = 1 / lenWght;
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    int rank = 0;
    for (int i = rs.size() - 1; i >= 0 && (numResults <= 0 || result.size() < numResults); --i) {
      int hit = keyList.getQuick(i);
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          IndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      HashSet<String> termSet = Sets.newHashSet(terms.getTerms());
      
      Float weight = result.get(termSet);
      if (weight == null) {
        // corpus level importance (added once)
        Document doc = fisIxReader.document(hit);
        
        float patternFreq = Float.parseFloat(doc.getFieldable(IndexBuilder.AssocField.SUPPORT.name)
            .stringValue());
        float patterIDF = (float) MathUtils.log(10, twtIxReader.numDocs() / patternFreq);
        
        float patternRank = Float.parseFloat(doc.getFieldable(IndexBuilder.AssocField.RANK.name)
            .stringValue());
        
        // (k + 1)
        // ------------------------------------- * idf * lambda
        // k * ((1-b) + b * (avgL / L)) + rank
        float delta = itemsetCorpusModelWeight * ((patterIDF * (FIS_BASE_RANK_PARAM_DEFAULT + 1))
            /
            (FIS_BASE_RANK_PARAM_DEFAULT
                * ((1 - lenWght) + (termSet.size() / itemsetLenghtAvg) * lenWght)
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
              * ((1 - lenWght) + (termSet.size() / itemsetLenghtAvg) * lenWght)
              + rank)) * (1 - itemsetCorpusModelWeight);
      weight += delta;
      
      result.put(termSet, weight);
    }
    return result;
  }
  
  public Query convertResultToBooleanQuery(OpenIntFloatHashMap rs, String query, int numResults)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    BooleanQuery result = new BooleanQuery();
    for (Entry<Set<String>, Float> e : convertResultToItemsets(rs, query, numResults)
        .entrySet()) {
      Query itemsetQuer = twtQparser.parse(e.getKey().toString().replaceAll("[\\,\\[\\]]", ""));
      itemsetQuer.setBoost(e.getValue());
      result.add(itemsetQuer, Occur.SHOULD);
    }
    return result;
  }
  
  public List<Query> convertResultToQueries(OpenIntFloatHashMap rs,
      String query, int numResults)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    List<Query> result = Lists.<Query> newLinkedList();
    for (Entry<Set<String>, Float> e : convertResultToItemsets(rs, query, numResults)
        .entrySet()) {
      Query itemsetQuer = twtQparser.parse(e.getKey().toString().replaceAll("[\\,\\[\\]]", ""));
      itemsetQuer.setBoost(e.getValue());
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
  
}
