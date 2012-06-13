package ca.uwaterloo.twitter.assoc.qexpand;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Set;
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
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class QueryExpander {
  private static Logger LOG = LoggerFactory.getLogger(QueryExpander.class);
  
  private static final String INDEX_OPTION = "index";
  
  // private static final String BASE_BOOST_OPTION = "base_boost";
  private static final float BASE_BOOST_DEFAULT = 10.0f;
  
  private static final String MIN_SCORE_OPTION = "base_boost";
  private static final float MIN_SCORE_DEFAULT = 1.0f;
  
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
    // .withDescription("boost that would be given to top level queries and reduced in lower levels").create(BASE_BOOST_OPTION));
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
    
    // float baseBoost = BASE_BOOST_DEFAULT;
    // try {
    // if (cmdline.hasOption(BASE_BOOST_OPTION)) {
    // baseBoost = Float.parseFloat(cmdline.getOptionValue(BASE_BOOST_OPTION));
    // }
    // } catch (NumberFormatException e) {
    // System.err.println("Invalid " + BASE_BOOST_OPTION + ": "
    // + cmdline.getOptionValue(BASE_BOOST_OPTION));
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
      
      TreeSet<ScoreIxObj<Integer>> rs = qEx.relatedItemsets(query.toString(), minScore);
      Pair<String[], Float>[] itemsets = qEx.convertResultToItemsets(rs);
      
      out.println();
      out.println(">" + query.toString());
      int i = 0;
      for (Pair<String[], Float> hit : itemsets) {
        out.println(++i + " (" + hit.getSecond() + "): " + Arrays.toString(hit.getFirst()));
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
  private float baseBoost = BASE_BOOST_DEFAULT;
  
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
  
  public TreeSet<ScoreIxObj<Integer>> relatedItemsets(String queryStr, float minScore)
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
          pairQuery.setBoost((2.0f / queryTerms.size()) * baseBoost);
          query.add(pairQuery, Occur.SHOULD);
        }
      }
      queryArr = null;
    }
    
    TreeSet<ScoreIxObj<Integer>> result = Sets.<ScoreIxObj<Integer>> newTreeSet();
    TopDocs rs = searcher.search(query, numHits);
    
    Set<ScoreIxObj<String>> extraTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int levelHits = addQualifiedResults(rs, result, queryTerms, extraTerms, minScore);
    LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    
    Set<ScoreIxObj<String>> doneTerms = Sets.<ScoreIxObj<String>> newHashSet();
    
    int level = 1;
    while (extraTerms.size() > 0 && result.size() < numHits) {
      float boost = (baseBoost - level) * (1.5f / queryTerms.size());
      if (boost <= 0) {
        break;
      }
      
      extraTerms = expandRecursive(queryTerms,
          extraTerms,
          doneTerms,
          result,
          numHits,
          minScore,
          boost);
      ++level;
    }
    
    // for (int i = levelHits; i < rs.scoreDocs.length && result.size() < numHits; ++i) {
    // result.add(new
    // ScoreIxObj<Integer>(rs.scoreDocs[i].doc,rs.scoreDocs[i].score,rs.scoreDocs[i].shardIndex));
    // }
    
    return result;
  }
  
  public Pair<String[], Float>[] convertResultToItemsets(TreeSet<ScoreIxObj<Integer>> rs)
      throws IOException {
    Pair<String[], Float>[] result = new Pair[rs.size()];
    
    int i = 0;
    for (ScoreIxObj<Integer> doc : rs) {
      TermFreqVector terms = ixReader.getTermFreqVector(doc.obj,
          IndexBuilder.AssocField.ITEMSET.name);
      result[i] = new Pair<String[], Float>(terms.getTerms(), doc.score);
      ++i;
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
      
      ScoreIxObj<Integer> scoreIxObj = new ScoreIxObj<Integer>(scoreDoc.doc,
          1.0f / (fusionK + rank), 
//          scoreDoc.score,
          scoreDoc.shardIndex);
      
      if (!result.contains(scoreIxObj)) {
        result.add(scoreIxObj);
        ++levelHits;        
      } else {
        result.remove(o)
      }
      
      TermFreqVector termVector = ixReader.getTermFreqVector(scoreDoc.doc,
          IndexBuilder.AssocField.ITEMSET.name);
      for (String term : termVector.getTerms()) {
        if (queryTerms.contains(term)) {
          continue;
        }
        extraTerms.add(new ScoreIxObj<String>(term, scoreDoc.score));
      }
    }
    
    return levelHits;
  }
  
  private Set<ScoreIxObj<String>> expandRecursive(Set<String> queryTerms,
      Set<ScoreIxObj<String>> extraTerms,
      Set<ScoreIxObj<String>> doneTerms,
      TreeSet<ScoreIxObj<Integer>> result, int numHits, float minScore, float boost)
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
      float qtermBoost = boost * similarity.idf(ixReader.docFreq(new
          Term(IndexBuilder.AssocField.ITEMSET.name, qterm)), ixReader.numDocs());
      for (ScoreIxObj<String> xterm : extraTerms) {
        assert !doneTerms.contains(xterm);
        
        Query xtermQuery = qparser.parse(qterm + "^" + qtermBoost + " " + xterm.obj + "^"
            + (xterm.score * boost));
        // Score doesn't have any effect this way
        // xtermQuery.setBoost(xterm.score * boost);
        
        TopDocs rs = searcher.search(xtermQuery, numHits);
        
        int levelHits = addQualifiedResults(rs, result, queryTerms, extraTerms2, minScore);
        LOG.debug("Added {} results from the query {}", levelHits, xtermQuery.toString());
      }
    }
    
    doneTerms.addAll(extraTerms);
    return Sets.difference(extraTerms2, doneTerms);
    
  }
  
}
