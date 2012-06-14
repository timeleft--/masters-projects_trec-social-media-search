package ca.uwaterloo.twitter.assoc.qexpand;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.analysis.Analyzer;
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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class QueryExpander {
  private static Logger LOG = LoggerFactory.getLogger(QueryExpander.class);
  
  private static final String INDEX_OPTION = "index";
  
  // private static final String BASE_PARAM_OPTION = "base_param";
  private static final float BASE_PARAM_DEFAULT = 60.0f;
  
  private static final String MIN_SCORE_OPTION = "min_score";
  private static final float MIN_SCORE_DEFAULT = Float.MIN_VALUE; //3.0f;
  
  private static final int NUM_HITS_DEFAULT = 100;
  
  private static final Analyzer ANALYZER = new ItemsetAnalyzer();// new
                                                                 // EnglishAnalyzer(Version.LUCENE_36);
  private static final boolean CHAR_BY_CHAR = false;
  
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
        .withDescription("index location").create(INDEX_OPTION));
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
    
    if (!cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(QueryExpander.class.getName(), options);
      System.exit(-1);
    }
    
    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    if (!indexLocation.exists()) {
      System.err.println("Error: " + indexLocation + " does not exist!");
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
    QueryExpander qEx = new QueryExpander(indexLocation);
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
      
      OpenIntFloatHashMap rs = qEx.relatedItemsets(query.toString(), minScore);
      LinkedHashMap<Set<String>, Float> itemsets = qEx.convertResultToItemsets(rs,-1);//100);
      
      out.println();
      out.println(">" + query.toString());
      int i = 0;
      for (Entry<Set<String>, Float> hit : itemsets.entrySet()) {
        out.println(++i + " (" + hit.getValue() + "): " + hit.getKey().toString());
        // + "\t"
        // + hit.get(IndexBuilder.AssocField.RANK.name) + "\t"
        // + hit.get(IndexBuilder.AssocField.SUPPORT.name));
      }
      
    } while (true);
    
  }
  
  private QueryParser qparser;
  private IndexSearcher searcher;
  private IndexReader ixReader;
  
  private int numHits = NUM_HITS_DEFAULT;
  private float baseRankingParam = BASE_PARAM_DEFAULT;
  
  private ItemSetSimilariry similarity;
  
  public QueryExpander(File indexLocation) throws IOException {
    Directory dir = new MMapDirectory(indexLocation);
    
    ixReader = IndexReader.open(dir);
    searcher = new IndexSearcher(ixReader);
    similarity = new ItemSetSimilariry();
    searcher.setSimilarity(similarity);
    
    qparser = new QueryParser(Version.LUCENE_36, IndexBuilder.AssocField.ITEMSET.name,
        ANALYZER);
    qparser.setDefaultOperator(Operator.AND);
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
    
    Query allQuery = qparser.parse(queryStr);
    // allQuery.setBoost(baseBoost);
    allQuery = allQuery.rewrite(ixReader);
    
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
          Query pairQuery = qparser.parse(queryArr[i] + " " + queryArr[j]);
          pairQuery.setBoost((2.0f / queryTerms.size())); // * baseBoost);
          query.add(pairQuery, Occur.SHOULD);
        }
      }
      queryArr = null;
    }
    
    OpenIntFloatHashMap resultSet = new OpenIntFloatHashMap();
    TopDocs rs = searcher.search(query, numHits);
    
    Set<ScoreIxObj<String>> extraTerms = Sets.<ScoreIxObj<String>>newHashSet();
    
    int levelHits = addQualifiedResults(rs, resultSet, queryTerms, extraTerms, minScore, baseRankingParam);
    LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    Set<ScoreIxObj<String>> doneTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int level = 0;
    while (extraTerms.size() > 0) {
      ++level;
      float fusionK = (float) (baseRankingParam + Math.pow(10, level));
      extraTerms = expandRecursive(queryTerms,
          extraTerms,
          doneTerms,
          resultSet,
          numHits,
          minScore,
          fusionK);
    }
    
    // for (int i = levelHits; i < rs.scoreDocs.length && result.size() < numHits; ++i) {
    // result.add(new
    // ScoreIxObj<Integer>(rs.scoreDocs[i].doc,rs.scoreDocs[i].score,rs.scoreDocs[i].shardIndex));
    // }
    
    return resultSet;
  }
  
  /**
   * Side effect: removes duplicates after converting docids to actual itemsets
   * @param rs
   * @return
   * @throws IOException
   */
  public LinkedHashMap<Set<String>, Float> convertResultToItemsets(OpenIntFloatHashMap rs, int numResults)
      throws IOException {
    LinkedHashMap<Set<String>, Float> result = Maps.<Set<String>, Float>newLinkedHashMap();
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    
    for(int i = rs.size()-1;i>=0 && (numResults <= 0 || result.size() < numResults); --i){
      int doc = keyList.getQuick(i);
      TermFreqVector terms = ixReader.getTermFreqVector(doc,
          IndexBuilder.AssocField.ITEMSET.name);
      result.put(Sets.newHashSet(terms.getTerms()), rs.get(doc));
    }
    // Could also use something like toString then .replaceAll("[\\,\\[\\]]", "");
   
    return result;
  }
  
  public void close() throws IOException {
    searcher.close();
    ixReader.close();
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
      
      if(!result.containsKey(scoreDoc.doc)){
        ++levelHits;    
      }
      
      float fusion = result.get(scoreDoc.doc);
      fusion += 1.0f / (fusionK + rank);
      result.put(scoreDoc.doc, fusion);
      
      TermFreqVector termVector = ixReader.getTermFreqVector(scoreDoc.doc,
          IndexBuilder.AssocField.ITEMSET.name);
      for (String term : termVector.getTerms()) {
        if (queryTerms.contains(term)) {
          continue;
        }
        // scoreDoc.score or fusion didn't change performance in a visible way
        extraTerms.add(new ScoreIxObj<String>(term,fusion)); 
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
    
    // This could have improvement speed, but it causes the scores to drop because of low overlap
    // even when multiplying the boost by the number of extra terms (to overcome the overlap)
    // for (String qterm : queryTerms) {
    // BooleanQuery query = new BooleanQuery(true);
    //
    // Query qtermQuery = qparser.parse(qterm);
    // qtermQuery.setBoost(similarity.idf(ixReader.docFreq(new
    // Term(IndexBuilder.AssocField.ITEMSET.name,qterm)),ixReader.numDocs()));
    //
    // query.add(qtermQuery, Occur.MUST);
    //
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    //
    // Query xtermQuery = qparser.parse(xterm.obj);
    // xtermQuery.setBoost(xterm.score); //* extraTerms.size());
    //
    // query.add(xtermQuery, Occur.SHOULD);
    // }
    //
    // // Does this have any effect at all?
    // query.setBoost(boost);
    //
    // TopDocs rs = searcher.search(query, numHits);
    //
    // int levelHits = addQualifiedResults(rs, result, queryTerms, extraTerms2, minScore);
    // LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    // }
    
    // This also improves performance by doing only one query (a long one)
    // but have an anecdotally low recall, a high precision though
    // BooleanQuery query = new BooleanQuery(); //This adds trash: true);
    // for (String qterm : queryTerms) {
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    //
    // Query xtermQuery = qparser.parse(qterm + " " + xterm.obj);
    // xtermQuery.setBoost(xterm.score);
    //
    // query.add(xtermQuery, Occur.SHOULD);
    // }
    // }
    // // Does this have any effect at all?
    // query.setBoost(boost);
    //
    // TopDocs rs = searcher.search(query, numHits);
    //
    // int levelHits = addQualifiedResults(rs, result, queryTerms, extraTerms2, minScore);
    // LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    for (String qterm : queryTerms) {
      for (ScoreIxObj<String> xterm : extraTerms) {
        assert !doneTerms.contains(xterm);
        
        Query xtermQuery = qparser.parse(qterm + " " + xterm.toString());
        
        TopDocs rs = searcher.search(xtermQuery, levelNumHits);
        
        int levelHits = addQualifiedResults(rs, resultSet, queryTerms, extraTerms2, levelMinScore, levelRankingParam);
        LOG.debug("Added {} results from the query {}", levelHits, xtermQuery.toString());
      }
    }
    
    doneTerms.addAll(extraTerms);
    return Sets.difference(extraTerms2, doneTerms); 
    
  }
  
}
