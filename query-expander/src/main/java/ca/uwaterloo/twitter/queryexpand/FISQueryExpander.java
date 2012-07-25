package ca.uwaterloo.twitter.queryexpand;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.util.MathUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TwitterEnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.TransactionTree;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.AbstractIntFloatMap;
import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weka.attributeSelection.LatentSemanticAnalysis;
import weka.clusterers.XMeans;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.matrix.Matrix;
import weka.core.matrix.SingularValueDecomposition;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.ReplaceMissingValues;
import ca.uwaterloo.timeseries.IncrementalChuncksParallelHiers;
import ca.uwaterloo.twitter.ItemSetIndexBuilder;
import ca.uwaterloo.twitter.ItemSetIndexBuilder.AssocField;
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

import edu.umd.cloud9.io.pair.PairOfInts;

public class FISQueryExpander {
  
  private static final Logger LOG = LoggerFactory.getLogger(FISQueryExpander.class);
  // HAHAHAH :D
  private static final double LOG2 = Math.log(2);
  
  static class FISCollector extends BM25Collector<Integer, String[]> {
    
    public class ScoreThenSuppRankComparator implements Comparator<ScoreIxObj<Integer>> {
      
      @Override
      public int compare(ScoreIxObj<Integer> o1, ScoreIxObj<Integer> o2) {
        int result = o1.compareTo(o2);
        if (result == 0) {
          try {
            Document doc1 = target.fisIxReader.document(o1.obj);
            Document doc2 = target.fisIxReader.document(o2.obj);
            String supp1 = doc1.get(AssocField.SUPPORT.name);
            String supp2 = doc2.get(AssocField.SUPPORT.name);
            
            result = -supp1.compareTo(supp2); // higher support is better
            
            if (result == 0) {
              String rank1 = doc1.get(AssocField.RANK.name);
              String rank2 = doc2.get(AssocField.RANK.name);
              
              result = rank1.compareTo(rank2); // lower rank is better
            }
          } catch (CorruptIndexException e) {
            LOG.error(e.getMessage(), e);
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
        }
        return result;
      }
    }
    
    private static final float AVG_PATTERN_LENGTH = 3.344369203831273f; // really
    
    public FISCollector(FISQueryExpander pTarget, String pQueryStr,
        OpenObjectFloatHashMap<String> pQueryTerms, float pQueryLen,
        int pMaxResults) throws IOException, IllegalArgumentException, SecurityException,
        InstantiationException, IllegalAccessException, InvocationTargetException {
      
      super(pTarget, (paramBM25StemmedIDF ? AssocField.STEMMED_EN.name : AssocField.ITEMSET.name),
          /* pQueryStr, */pQueryTerms, pQueryLen, 0,
          pMaxResults, ScoreThenSuppRankComparator.class, paramBM25StemmedIDF);
      
      clarityScore = true;
    }
    
    @Override
    protected Integer getResultKey(int docId, Document doc) {
      return docBase + docId;
    }
    
    @Override
    protected String[] getResultValue(int docId, Document doc) {
      return docTerms.keys().toArray(new String[0]);
    }
    
    // Those are actually irrelevant given that clarity score is used
    // @Override
    // protected float getB() {
    // return 0.0f; // Longer is better for now
    // }
    //
    // @Override
    // protected float getK1() {
    // return 0.0f;
    // }
    //
    @Override
    protected float getLAVG() {
      // This will not have any effect as long as B is 0
      return AVG_PATTERN_LENGTH;
    }
    
  }
  
  public static final TextCategorizer textcat = new TextCategorizer(FileUtils
      .toFile(FISQueryExpander.class.getResource("/textcat.conf")).toString());
  
  public static final TwitterEnglishAnalyzer tweetStemmingAnalyzer = new TwitterEnglishAnalyzer();
  public static final TwitterAnalyzer tweetNonStemmingAnalyzer = new TwitterAnalyzer();
  
  public static class QueryExpansionBM25Collector extends BM25Collector<String, String> {
    
    // protected static final float FREQ_PERCENTILE = 0.90f;
    protected boolean languageIdentification = PARAM_LANGUAGE_IDENTIFICATION;
    
    public boolean isLanguageIdentification() {
      return languageIdentification;
    }
    
    public void setLanguageIdentification(boolean languageIdentification) {
      this.languageIdentification = languageIdentification;
    }
    
    public QueryExpansionBM25Collector(FISQueryExpander pTarget, String pDocTextField,
        /* String pQueryStr, */OpenObjectFloatHashMap<String> pQueryTerms, float pQueryLen,
        int addNEnglishStopWordsToQueryTerms, int pMaxResults,
        Class<? extends Comparator<ScoreIxObj<String>>> comparatorClazz, boolean pStemmedIDF)
        throws IOException, IllegalArgumentException, SecurityException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
      super(pTarget, pDocTextField, /* pQueryStr, */pQueryTerms, pQueryLen,
          addNEnglishStopWordsToQueryTerms,
          pMaxResults, comparatorClazz, pStemmedIDF);
    }
    
    public PriorityQueue<ScoreIxObj<String>>[] expansionTermsByClusterTopResults(
        int numResultsToUse,
        List<MutableFloat> minXTermScoresOut, List<MutableFloat> maxXTermScoresOut,
        List<MutableFloat> totalXTermScoresOut, String queryStr) throws Exception {
      
      if (numResultsToUse <= 0) {
        throw new IllegalArgumentException("Must specify number of results");
      }
      
      OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
      OpenObjectFloatHashMap<String> termRSFreq = new OpenObjectFloatHashMap<String>();
      float totalRSFreq = 0;
      List<String> idTermMap = Lists.newArrayList();
      // List<Term> stemmedList = Lists.newArrayList();
      LinkedHashSet<Set<String>> itemsetsSet = new LinkedHashSet<Set<String>>();
      FastVector attrs = new FastVector();
      
      List<OpenIntFloatHashMap> bWordDocTemp = Lists.newArrayList();
      
      int rank = -1;
      int nextId = 0;
      
      for (ScoreIxObj<String> key : resultSet.keySet()) {
        if (++rank >= numResultsToUse) {
          break;
        }
        
        String tweet = resultSet.get(key);
        MutableLong docLen = new MutableLong();
        OpenObjectFloatHashMap<String> termDocFreq = target.queryTermFreq(tweet,
            docLen,
            tweetStemmingAnalyzer,
            TweetField.STEMMED_EN.name);
        // tweetNonStemmingAnalyzer,
        // TweetField.TEXT.name);
        itemsetsSet.add(Sets.newCopyOnWriteArraySet(termDocFreq.keys()));
        
        if (termDocFreq.size() < MIN_ITEMSET_SIZE) { // no repetition .containsKey(termSet)) {
          continue;
        }
        
        // IntArrayList pattern = new IntArrayList(termSet.size());
        
        for (String term : termDocFreq.keys()) {
          int termId;
          if (!termIdMap.containsKey(term)) {
            termId = nextId++;
            termIdMap.put(term, termId);
            idTermMap.add(term);
            bWordDocTemp.add(new OpenIntFloatHashMap());
            // String fieldname = TweetField.STEMMED_EN.name;
            // stemmedList.add();
            attrs.addElement(new Attribute(term));
          }
          termId = termIdMap.get(term);
          
          // Term stemmed = stemmedList.get(termId);
          Term stemmed = new Term(TweetField.STEMMED_EN.name, term);
          
          // /// HEY THESE ARE MY OWN APPROXIMATIONSS >>> HOPE FOR THE BESTT!!!!!
          float globalTi;
          // TODO revise smoothing
          globalTi = (target.termWeightSmoother + target.twtIxReader.docFreq(stemmed))
              / (target.termWeightSmoother + TWITTER_CORPUS_LENGTH_IN_TERMS);
          
          bWordDocTemp.get(termId).put(rank, globalTi);
          
          float freq = termDocFreq.get(term);
          termRSFreq.put(term, termRSFreq.get(term) + freq);
          totalRSFreq += freq;
        }
      }
      
      if (bWordDocTemp.size() == 0) {
        return new PriorityQueue[0];
      }
      // In case rank is less
      numResultsToUse = rank;
      
      SummaryStatistics[] colStats = new SummaryStatistics[numResultsToUse];
      
      double[][] bPWD = new double[bWordDocTemp.size()][numResultsToUse];
      for (int d = 0; d < numResultsToUse; ++d) {
        colStats[d] = new SummaryStatistics();
        for (int w = 0; w < bWordDocTemp.size(); ++w) {
          bPWD[w][d] = bWordDocTemp.get(w).get(d);
          float rsIDF = termRSFreq.get(idTermMap.get(w)) / totalRSFreq;
          float corpusIDF;
          corpusIDF = 1.0f
              * target.twtIxReader.docFreq(new Term(TweetField.STEMMED_EN.name, idTermMap.get(w)))
              / target.twtIxReader.numDocs();
          if (corpusIDF == 0) {
            // shit happens!!!
            bPWD[w][d] = 0;
          } else {
            bPWD[w][d] *= Math.log(rsIDF / corpusIDF);
          }
          colStats[d].addValue(bPWD[w][d]);
        }
      }
      Instances insts = new Instances("tweet-term", attrs, numResultsToUse);
      for (int d = 0; d < numResultsToUse; ++d) {
        Instance instance = new Instance(attrs.size());
        for (int w = 0; w < bWordDocTemp.size(); ++w) {
          instance.setValue(w, bPWD[w][d] - colStats[d].getMean()); // TODO divide by standard
                                                                    // deviation??
        }
        
        insts.add(instance);
      }
      
      XMeans clusterer = new XMeans();
      clusterer.setDistanceF(new CosineDistance());
      clusterer.buildClusterer(insts);
      
      OpenObjectFloatHashMap<String>[] termWeights = new OpenObjectFloatHashMap[clusterer
          .numberOfClusters()];
      for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
        termWeights[c] = new OpenObjectFloatHashMap<String>();
        if (minXTermScoresOut != null)
          minXTermScoresOut.add(new MutableFloat(Float.MAX_VALUE));
        
        if (maxXTermScoresOut != null)
          maxXTermScoresOut.add(new MutableFloat(Float.MIN_VALUE));
        
        if (totalXTermScoresOut != null)
          totalXTermScoresOut.add(new MutableFloat(0));
      }
      
      int d = -1;
      for (Set<String> patternItems : itemsetsSet) {
        ++d;
        if (d >= numResultsToUse) {
          break;
        }
        Instance inst = insts.instance(d);
        for (String item : patternItems) {
          int termId = termIdMap.get(item);
          String term = ((Attribute) attrs.elementAt(termId)).name();
          double[] distrib = clusterer.distributionForInstance(inst);
          for (int c = 0; c < distrib.length; ++c) {
            if (distrib[c] <= CLUSTER_MEMBERSHIP_THRESHOLD) {
              continue;
            }
            
            // Closeness to centroid
            Instance centroid = clusterer.getClusterCenters().instance(c);
            float score = 1 - (float) clusterer.getDistanceF().distance(centroid, inst);
            
            score += termWeights[c].get(term);
            termWeights[c].put(term, score);
            
            if (minXTermScoresOut != null) {
              if (score < minXTermScoresOut.get(c).floatValue()) {
                minXTermScoresOut.get(c).setValue(score);
              }
            }
            
            if (maxXTermScoresOut != null) {
              if (score > maxXTermScoresOut.get(c).floatValue()) {
                maxXTermScoresOut.get(c).setValue(score);
              }
            }
            
            if (totalXTermScoresOut != null) {
              totalXTermScoresOut.get(c).add(score);
            }
          }
        }
      }
      PriorityQueue<ScoreIxObj<String>>[] result = new PriorityQueue[clusterer.numberOfClusters()];
      OpenObjectFloatHashMap<String> queryTerms = target.queryTermFreq(queryStr,
          null,
          tweetStemmingAnalyzer,
          TweetField.STEMMED_EN.name);
      // tweetNonStemmingAnalyzer,
      // TweetField.TEXT.name);
      
      for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
        result[c] = new PriorityQueue<ScoreIxObj<String>>();
        for (String term : termWeights[c].keys()) {
          float score = termWeights[c].get(term);
          if (queryTerms.containsKey(term) || score == 0) {
            continue;
          }
          result[c].add(new ScoreIxObj<String>(term, score));
        }
      }
      
      return result;
    }
    
    public OpenObjectFloatHashMap<String> expansionTermsByIDFIncrease(int numResultsToCosider,
        MutableFloat minScoreOut, MutableFloat maxScoreOut, MutableFloat totalScoreOut)
        throws IOException {
      
      OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>();
      
      if (minScoreOut != null) {
        minScoreOut.setValue(Float.MAX_VALUE);
      }
      
      if (maxScoreOut != null) {
        maxScoreOut.setValue(Float.MIN_VALUE);
      }
      
      if (totalScoreOut != null) {
        totalScoreOut.setValue(0);
      }
      
      // float maxFreq = Float.MIN_VALUE;
      // float totalFreq = 0;
      int rank = 0;
      for (ScoreIxObj<String> doc : resultSet.keySet()) {
        if (numResultsToCosider > 0 && rank >= numResultsToCosider) {
          break;
        }
        
        OpenObjectFloatHashMap<String> hitTerms = target.queryTermFreq(resultSet.get(doc),
            null,
            tweetStemmingAnalyzer,
            TweetField.STEMMED_EN.name);
        // tweetNonStemmingAnalyzer,
        // TweetField.TEXT.name);
        if ((hitTerms.size() < MIN_ITEMSET_SIZE)) { // TODO: duplicates? || (.containsKey(termSet)
          continue;
        }
        
        for (String term : hitTerms.keys()) {
          
          if (queryTerms.containsKey(term)) {
            continue;
          }
          
          // IDF:
          termFreq.put(term, termFreq.get(term) + 1);
          
          // Frequency
          // float score = hitTerms.get(term);
          // totalFreq += score;
          //
          // score += termFreq.get(term);
          // termFreq.put(term, score);
          
          // if (score > maxFreq) {
          // maxFreq = score;
          // }
          
        }
        
        ++rank;
      }
      
      // List<String> termsByFreq = Lists.newArrayListWithCapacity(termFreq.size());
      // termFreq.keys(termsByFreq);
      // int maxIx = (int) (FREQ_PERCENTILE * termFreq.size());
      // maxFreq = termFreq.get(termsByFreq.get(maxIx));
      //
      // for (int i = maxIx + 1; i < termsByFreq.size(); ++i) {
      // String term = termsByFreq.get(i);
      // if (termFreq.get(term) > maxFreq) {
      // termFreq.removeKey(term);
      // }
      // }
      
      for (String term : termFreq.keys()) {
        Term stemmed = new Term(TweetField.STEMMED_EN.name, term);
        // already stemmed
        // target.queryTermFreq(term, null,
        // // tweetStemmingAnalyzer, TweetField.STEMMED_EN.name)
        // tweetNonStemmingAnalyzer,
        // TweetField.TEXT.name)
        // .keys()
        // .get(0));
        float docFreq = target.twtIxReader.docFreq(stemmed);
        float idfGlobal = 1.0f * docFreq / target.twtIxReader.numDocs();
        float idfLocal = termFreq.get(term) / rank;
        
        float score = (float) Math.log(idfLocal / idfGlobal);
        
        // Pulls out stop words
        // float tfGlobal = (docFreq + 10000) / (TWITTER_CORPUS_LENGTH_IN_TERMS); //) + 10000);
        // score *= tfGlobal;
        
        termFreq.put(term, score);
        
        if (minScoreOut != null && score < minScoreOut.floatValue()) {
          minScoreOut.setValue(score);
        }
        
        if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
          maxScoreOut.setValue(score);
        }
        
        if (totalScoreOut != null) {
          totalScoreOut.add(score);
        }
      }
      return termFreq;
    }
    
    // This is slow as peppeee
    // @Override
    // public void collect(int docId) throws IOException {
    // if (languageIdentification) {
    //
    // if (encounteredDocs.contains(docId)) {
    // LOG.trace("Duplicate document {} for query {}", docId, queryStr);
    // return;
    // }
    //
    // Document doc = reader.document(docId);
    //
    // String tweet = doc.get(TweetField.TEXT.name);
    // String lang = textcat.categorize(tweet);
    // if ("english".equals(lang)) {
    // super.collect(docId);
    // } else {
    // encounteredDocs.add(docId);
    // LOG.debug("Neglecting ({}) tweet: {}", lang, tweet);
    // }
    // } else {
    // super.collect(docId);
    // }
    // }
    
    @Override
    protected String getResultKey(int docId, Document doc) {
      String tweetId = doc.get(TweetField.ID.name);
      return tweetId;
    }
    
    @Override
    protected String getResultValue(int docId, Document doc) {
      String text = null;
      if (languageIdentification || LOG.isDebugEnabled()) {
        text = doc.get(TweetField.TEXT.name);
      }
      return text;
    }
  }
  
  public static enum TermWeigting {
    PROB_QUERY("pq"),
    KL_DIVERG("kl"),
    CLUSTERING_TERMS("clustering_term-coccurr"),
    SVD_PATTERN("svd_pattern"),
    SVD_TERMCOCC("svd_term-cocc"),
    FREQ("frequency"),
    TOPN("topn_pattern-rank"),
    MARKOV("lafferty-zhai-2001"),
    MUTUAL_ENTROPY("ibm-china"), CLUSTERING_PATTERNS("duh");
    
    public final String name;
    
    TermWeigting(String s) {
      name = s;
    }
  };
  
  public enum QueryParseMode {
    CONJUNGTIVE("AND"),
    DISJUNCTIVE("OR"),
    POWERSET("powerset"), // All non-empty susbsets
    BIGRAMS("bigrams"); // TODO
    
    String name;
    
    private QueryParseMode(String pName) {
      name = pName;
    }
    
    @Override
    public String toString() {
      return name;
    }
  }
  
  public enum ExpandMode {
    FILTERING("filter"),
    DIVERSITY("diverse");
    
    String name;
    
    private ExpandMode(String pName) {
      name = pName;
    }
    
    @Override
    public String toString() {
      return name;
    }
  }
  
  private static final String FIS_INDEX_OPTION = "fis_inc_index";
  private static final String TWT_INC_INDEX_OPTION = "twt_inc_index";
  private static final String TWT_CHUNK_INDEX_OPTION = "twt_chunk_index";
  
  // private static final String BASE_PARAM_OPTION = "base_param";
  private static final float FIS_BASE_RANK_PARAM_DEFAULT = 60.0f;
  
  private static final String MIN_SCORE_OPTION = "min_score";
  private static final float MIN_SCORE_DEFAULT = Float.MIN_VALUE; // very sensitive!
  // Whjen I was using lucene scorer I had to guess and try.. I was dumb!
  // // Must be large, because otherwise the topic drifts quickly...
  // // it seems that the top itemsets are repetetive, so we need more breadth
  private static final int NUM_HITS_INTERNAL_DEFAULT = 1000;
  // When using the lucene scorer it was an observational experiment.. I thought I was doing magic
  // :(
  // // This is enough recall and the concept doesn't drift much.. one more level pulls trash..
  // // less is more precise, but I'm afraid won't have enough recall; will use score to block trash
  private static final int MAX_LEVELS_EXPANSION = 1;
  private static final int NUM_HITS_SHOWN_DEFAULT = 1000;
  
  // private static final Analyzer ANALYZER = new TwitterAnalyzer();// new
  // // EnglishAnalyzer(Version.LUCENE_36);
  private static final boolean CHAR_BY_CHAR = false;
  
  private static final int MIN_ITEMSET_SIZE = 1;
  
  private static final String RETWEET_TERM = "rt";
  
  private static final float ITEMSET_LEN_AVG_DEFAULT = 5;// FIXME: there is a correct number in
                                                         // FISCollector
  
  private static final float ITEMSET_LEN_WEIGHT_DEFAULT = 0.33f;
  
  private static final float ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT = 0.77f;
  private static final float TWITTER_CORPUS_MODEL_WEIGHT_DEFAULT = 0.33f;
  
  private static final float TERM_WEIGHT_SMOOTHER_DEFAULT = 10000.0f;
  
  private static final float SCORE_PRECISION_MULTIPLIER = 100000.0f;
  
  private static final TermWeigting TERM_WEIGHTIN_DEFAULT = TermWeigting.CLUSTERING_PATTERNS;
  
  private static final String COLLECTION_STRING_CLEANER = "[\\,\\[\\]]";
  
  private static final boolean QUERY_SUBSET_BOOST_IDF_DEFAULT = true;
  
  private static final boolean QUERY_SUBSET_BOOST_YESNO_DEFAULT = false;
  
  private static final boolean QUERY_PARSE_INTO_TERMS_DEFAULT = true;
  
  private static final int ENGLISH_STOPWORDS_COUNT = 5;
  private static final boolean DEAFULT_MAGIC_ALLOWED = false;
  private static final boolean EXPAND_TERM_COUNT_EVENLY_OVER_CLUSTERS = false;
  private static final boolean REAL_TIME_PATTERN_FREQ = false;
  public static final boolean PROPAGATE_IS_WEIGHTS_DEFAULT = true;
  private static final float TWITTER_CORPUS_LENGTH_IN_TERMS = 100055949;
  private static final float ITEMSET_CORPUS_LENGTH_IN_TERMS = 5760589;
  public static final double PARAM_MARKOV_ALPHA = 0.5;
  public static final int PARAM_MARKOV_NUM_WALK_STEPS = 2;
  private static final boolean PARAM_PROB_DOC_FROM_TWITTER = false;
  private static final boolean PARAM_MUTUAL_ENTROPY_START_WITH_ENTROPY = false;
  private static final boolean PARAM_CLUSTERING_APPLY_LSA = false;
  private static final boolean PARAM_LANGUAGE_IDENTIFICATION = false;
  // WARNING: if you set that to false I don't know what will happen.. not kept supported
  public static final boolean SEARCH_NON_STEMMED = true;
  
  private static boolean paramClusteringWeightInsts = true;
  
  private static boolean paramBM25StemmedIDF = true;
  
  /**
   * @param args
   * @throws java.text.ParseException
   * @throws Exception
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
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
    QueryParser twtQparser = new QueryParser(Version.LUCENE_36, TweetField.TEXT.name,
        tweetNonStemmingAnalyzer);
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
        } else if (cmd.equals("l:")) {
          mode = 1; // close itemsets
        } else if (cmd.equals("n:")) {
          mode = 2;
          termWeighting = TermWeigting.TOPN;
        } else if (cmd.equals("f:")) {
          mode = 3; // terms with query divergence
          termWeighting = TermWeigting.FREQ;
        } else if (cmd.equals("s:")) {
          mode = 4; //
          termWeighting = TermWeigting.SVD_PATTERN;
        } else if (cmd.equals("h:")) {
          mode = 5;
          termWeighting = TermWeigting.MUTUAL_ENTROPY;
        } else if (cmd.equals("q:")) {
          mode = 6; //
          termWeighting = TermWeigting.PROB_QUERY;
        } else if (cmd.equals("d:")) {
          mode = 7; //
          termWeighting = TermWeigting.KL_DIVERG;
        } else if (cmd.equals("m:")) {
          mode = 8; //
          termWeighting = TermWeigting.MARKOV;
        } else if (cmd.equals("v:")) {
          mode = 9;
          termWeighting = TermWeigting.SVD_TERMCOCC;
        } else if (cmd.equals("r:")) {
          mode = 10; // results
        } else if (cmd.equals("e:")) {
          mode = 20; // expanded
        } else if (cmd.equals("t:")) {
          mode = 30; // set the time
        } else if (cmd.equals("c:")) {
          mode = 50; //
          termWeighting = TermWeigting.CLUSTERING_TERMS;
        } else if (cmd.equals("x:")) {
          mode = 52;
          termWeighting = TermWeigting.CLUSTERING_PATTERNS;
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
        
        // Query parsedQuery = qEx.twtQparser.parse(query.toString());
        Query parsedQuery = twtQparser.parse(query.toString());
        
        if (mode == 0) {
          IntArrayList keyList = new IntArrayList(fisRs.size());
          fisRs.keysSortedByValue(keyList);
          int rank = 0;
          for (int i = fisRs.size() - 1; i >= 0; --i) {
            int hit = keyList.getQuick(i);
            TermFreqVector terms = qEx.fisIxReader.getTermFreqVector(hit,
                ItemSetIndexBuilder.AssocField.ITEMSET.name);
            out.println(++rank + " (" + fisRs.get(hit) + "): " + Arrays.toString(terms.getTerms()));
          }
        } else if (mode == 1) {
          PriorityQueue<ScoreIxObj<List<String>>> itemsets = qEx.convertResultToItemsets(fisRs,
              query.toString(),
              -1);
          // NUM_HITS_DEFAULT);
          int i = 0;
          while (!itemsets.isEmpty()) {
            ScoreIxObj<List<String>> is = itemsets.poll();
            out.println(++i + " (" + is.score + "): " + is.obj.toString());
          }
        } else if (mode == 50 || mode == 52) {
          // PriorityQueue<ScoreIxObj<String>>[] clusterTerms = qEx
          // .convertResultToWeightedTermsByClusteringPatterns(fisRs,
          // query.toString(),
          // clusterClosedOnly,
          // null,
          // null,
          // null,
          // paramClusteringWeightInsts);
          PriorityQueue<ScoreIxObj<String>>[] clusterTerms = null;
          if (TermWeigting.CLUSTERING_TERMS.equals(termWeighting)) {
            clusterTerms = qEx
                .weightedTermsByClusteringTerms(fisRs, query.toString(), 100, null, null, null);
          } else if (TermWeigting.CLUSTERING_PATTERNS.equals(termWeighting)) {
            clusterTerms = qEx
                .weightedTermsClusterPatterns(fisRs,
                    query.toString(),
                    100,
                    null,
                    null,
                    null,
                    false,
                    false);
          }
          int i = 0;
          OpenIntHashSet empty = new OpenIntHashSet();
          while (empty.size() < clusterTerms.length) {
            for (int c = 0; c < clusterTerms.length; ++c, ++i) {
              if (c > 0) {
                out.print("\t");
              }
              if (clusterTerms[c].isEmpty()) {
                empty.add(c);
                continue;
              }
              ScoreIxObj<String> t = clusterTerms[c].poll();
              out.print(c + ":" + (i / clusterTerms.length) + " (" + t.score + "): " + t.obj);
            }
            out.print("\n");
          }
        } else if (mode == 2 || mode == 3 || mode == 4 || mode == 5 || mode == 6 || mode == 7
            || mode == 8 || mode == 9) {
          OpenObjectFloatHashMap<String> weightedTerms = null;
          if (TermWeigting.FREQ.equals(termWeighting)) {
            weightedTerms = qEx
                .weightedTermsByFreq(fisRs,
                    query.toString(),
                    -1,
                    null,
                    null,
                    null,
                    PROPAGATE_IS_WEIGHTS_DEFAULT);
          } else if (TermWeigting.TOPN.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsByPatternRank(fisRs,
                query.toString(),
                -1,
                null,
                null,
                null);
          } else if (TermWeigting.PROB_QUERY.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsConditionalProb(fisRs,
                query.toString(),
                -1,
                // PROPAGATE_IS_WEIGHTS_DEFAULT,
                null,
                null,
                null);
          } else if (TermWeigting.MARKOV.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsMarkovChain(fisRs,
                query.toString(),
                100,
                null,
                null,
                null,
                PARAM_MARKOV_NUM_WALK_STEPS,
                PARAM_MARKOV_ALPHA, PARAM_PROB_DOC_FROM_TWITTER);
          } else if (TermWeigting.SVD_PATTERN.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsSVDOfPatternMatrix(fisRs,
                query.toString(),
                1000,
                null,
                null,
                null,
                PARAM_PROB_DOC_FROM_TWITTER);
            
          } else if (TermWeigting.MUTUAL_ENTROPY.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsByConditionalEntropy(fisRs,
                query.toString(),
                77,
                33,
                null,
                null,
                null,
                PARAM_MUTUAL_ENTROPY_START_WITH_ENTROPY);
          } else if (TermWeigting.SVD_TERMCOCC.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsBySVDOfTermCoocur(fisRs,
                query.toString(),
                100,
                null,
                null,
                null);
          } else if (TermWeigting.KL_DIVERG.equals(termWeighting)) {
            weightedTerms = qEx.weightedTermsByKLDivergence(fisRs,
                query.toString(),
                100,
                null,
                null,
                null,
                false);
            
          }
          
          List<String> termList = Lists.newArrayListWithCapacity(weightedTerms.size());
          weightedTerms.keysSortedByValue(termList);
          
          int i = 0;
          for (String term : termList) {
            out.println(++i + " (" + weightedTerms.get(term) + "): " + term);
          }
        } else if (mode == -1) {
          
          PriorityQueue<ScoreIxObj<String>> weightedTerms = null;
          // if (TermWeigting.PROB_QUERY.equals(termWeighting)) {
          // weightedTerms = qEx
          // .convertResultToWeightedTermsConditionalProb(fisRs,
          // query.toString(),
          // -1, propagateISWeights, null, null, null);
          // } else
          // if (TermWeigting.KL_DIVERG.equals(termWeighting)) {
          // weightedTerms = qEx
          // .convertResultToWeightedTermsKLDivergence(fisRs,
          // query.toString(),
          // -1, propagateISWeights, null, null, null);
          // }
          // else if (TermWeigting.SVD.equals(termWeighting)) {
          // weightedTerms = qEx
          // .convertResultToWeightedTermsBySVDOfTerms(
          // // PatternsMatrix(
          // fisRs,
          // true,
          // paramClusteringWeightInsts);
          // }
          
          int i = 0;
          while (!weightedTerms.isEmpty()) {
            ScoreIxObj<String> t = weightedTerms.poll();
            out.println(++i + " (" + t.score + "): " + t.obj);
          }
          
        } else {
          Query twtQ = null;
          // = new BooleanQuery();
          // twtQ.add(qEx.twtQparser.parse(RETWEET_QUERY).rewrite(qEx.twtIxReader), Occur.MUST_NOT);
          
          if (mode == 10) {
            twtQ = qEx.filterQuery(parsedQuery);
          } else if (mode == 20) {
            throw new UnsupportedOperationException();
            // // twtQ.add(qEx.convertResultToBooleanQuery(fisRs,
            // // query.toString(),
            // // NUM_HITS_INTERNAL_DEFAULT),
            // // Occur.SHOULD);
            // // twtQ.setMinimumNumberShouldMatch(1);
            //
            // List<MutableFloat> minXTermScores = Lists.newArrayList();
            // List<MutableFloat> maxXTermScores = Lists.newArrayList();
            // List<MutableFloat> totalXTermScores = Lists.newArrayList();
            //
            // PriorityQueue<ScoreIxObj<String>>[] clustersTerms = qEx
            // .convertResultToWeightedTermsByClusteringPatterns(fisRs, query.toString(), true,
            // minXTermScores, maxXTermScores, totalXTermScores, paramClusteringWeightInsts);
            //
            // MutableLong queryLen = new MutableLong();
            // OpenObjectFloatHashMap<String> queryTerms = qEx.queryTermFreq(query.toString(),
            // queryLen, tweetStemmingAnalyzer,
            // TweetField.STEMMED_EN.name);
            // twtQ = qEx.expandAndFilterQuery(queryTerms,
            // queryLen.intValue(),
            // clustersTerms,
            // minXTermScores.toArray(new MutableFloat[0]),
            // maxXTermScores.toArray(new MutableFloat[0]),
            // clustersTerms.length * 7,
            // null,
            // null,
            // ExpandMode.DIVERSITY);
          }
          
          LOG.debug("Querying Twitter by: " + twtQ.toString());
          int t = 0;
          for (ScoreDoc scoreDoc : qEx.twtSearcher.search(twtQ, NUM_HITS_SHOWN_DEFAULT).scoreDocs) {
            Document hit = qEx.twtIxReader.document(scoreDoc.doc);
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
  
  protected Pair<Set<String>, Float> getPattern(int docid) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    TermFreqVector terms = fisIxReader.getTermFreqVector(docid,
        ItemSetIndexBuilder.AssocField.ITEMSET.name);
    Set<String> termSet = Sets.newCopyOnWriteArraySet(Arrays.asList(terms.getTerms()));
    float freq;
    if (REAL_TIME_PATTERN_FREQ) {
      throw new UnsupportedOperationException();
      // Query countQuery =
      // twtQparser.parse(termSet.toString().replaceAll(COLLECTION_STRING_CLEANER,
      // ""));
      // countQuery = filterQuery(countQuery);
      // final MutableFloat count = new MutableFloat(0);
      // twtSearcher.search(countQuery, new Collector() {
      // // counting collector
      // @Override
      // public void setScorer(Scorer scorer) throws IOException {
      // }
      //
      // @Override
      // public void setNextReader(IndexReader reader, int docBase) throws IOException {
      // }
      //
      // @Override
      // public void collect(int doc) throws IOException {
      // count.add(1);
      // }
      //
      // @Override
      // public boolean acceptsDocsOutOfOrder() {
      // return true;
      // }
      // });
      // freq = count.floatValue();
      // if(freq < MINSUPPORT) freq = 0;
    } else {
      Document doc = fisIxReader.document(docid);
      freq = Float.parseFloat(doc
          .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
          .stringValue());
    }
    return new Pair<Set<String>, Float>(termSet, freq);
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsByPatternRank(
      OpenIntFloatHashMap fisRs, String query, int numTermsToReturn,
      MutableFloat minScoreOut, MutableFloat maxScoreOut, MutableFloat totalScoreOut)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    if (numTermsToReturn <= 0) {
      numTermsToReturn = Integer.MAX_VALUE;
    }
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    if (minScoreOut != null) {
      minScoreOut.setValue(Float.MAX_VALUE);
    }
    
    if (maxScoreOut != null) {
      maxScoreOut.setValue(Float.MIN_VALUE);
    }
    
    LinkedHashMap<Set<String>, Float> rankedItemsetScore = Maps.newLinkedHashMap();
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    for (int i = fisRs.size() - 1; i >= 0 && numTermsToReturn > 0; --i) {
      int hit = keyList.getQuick(i);
      
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      if ((termSet.size() < MIN_ITEMSET_SIZE) || (rankedItemsetScore.containsKey(termSet))) {
        continue;
      }
      
      float patternFreq = patternPair.getSecond();
      
      rankedItemsetScore.put(termSet, patternFreq);
      
      for (String term : termSet) {
        
        if (result.containsKey(term) || queryTerms.containsKey(term)) {
          continue;
        }
        
        float score = numTermsToReturn--;
        result.put(term, score);
        
        if (minScoreOut != null && score < minScoreOut.floatValue()) {
          minScoreOut.setValue(score);
        }
        
        if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
          maxScoreOut.setValue(score);
        }
        
        if (totalScoreOut != null) {
          totalScoreOut.add(score);
        }
      }
      
    }
    
    return result;
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsByFreq(
      OpenIntFloatHashMap fisRs, String query, int numItemsetsToConsider,
      MutableFloat minScoreOut, MutableFloat maxScoreOut, MutableFloat totalScoreOut,
      boolean propagateSupport)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>();
    
    if (minScoreOut != null) {
      minScoreOut.setValue(Float.MAX_VALUE);
    }
    
    if (maxScoreOut != null) {
      maxScoreOut.setValue(Float.MIN_VALUE);
    }
    
    LinkedHashMap<Set<String>, Float> rankedItemsetScore = Maps.newLinkedHashMap();
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    int rank = 1;
    for (int i = fisRs.size() - 1; i >= 0; --i) {
      if (numItemsetsToConsider > 0 && rank > numItemsetsToConsider) {
        break;
      }
      int hit = keyList.getQuick(i);
      
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      if ((termSet.size() < MIN_ITEMSET_SIZE) || (rankedItemsetScore.containsKey(termSet))) {
        continue;
      }
      
      float patternFreq = patternPair.getSecond();
      
      rankedItemsetScore.put(termSet, patternFreq);
      
      for (String term : termSet) {
        
        if (queryTerms.containsKey(term)) {
          continue;
        }
        
        float score = (propagateSupport ? patternFreq : 1) + termFreq.get(term);
        termFreq.put(term, score);
        
        if (minScoreOut != null && score < minScoreOut.floatValue()) {
          minScoreOut.setValue(score);
        }
        
        if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
          maxScoreOut.setValue(score);
        }
        
        if (totalScoreOut != null) {
          totalScoreOut.add(score);
        }
      }
      
      ++rank;
    }
    
    return termFreq;
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsByKLDivergence(
      OpenIntFloatHashMap fisRs, String query, int numItemsetsToConsider,
      MutableFloat minScoreOut, MutableFloat maxScoreOut, MutableFloat totalScoreOut,
      boolean pdwFromTwitter)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>();
    
    if (minScoreOut != null) {
      minScoreOut.setValue(Float.MAX_VALUE);
    }
    
    if (maxScoreOut != null) {
      maxScoreOut.setValue(Float.MIN_VALUE);
    }
    
    LinkedHashMap<Set<String>, Float> rankedItemsetScore = Maps.newLinkedHashMap();
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    float totalSupport = 0;
    int rank = 1;
    for (int i = fisRs.size() - 1; i >= 0; --i) {
      if (numItemsetsToConsider > 0 && rank > numItemsetsToConsider) {
        break;
      }
      int hit = keyList.getQuick(i);
      
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      if ((termSet.size() < MIN_ITEMSET_SIZE) || (rankedItemsetScore.containsKey(termSet))) {
        continue;
      }
      
      float patternFreq = patternPair.getSecond();
      
      rankedItemsetScore.put(termSet, patternFreq);
      
      for (String term : termSet) {
        
        if (queryTerms.containsKey(term)) {
          continue;
        }
        
        boolean propagateSupport = true;
        float support = (propagateSupport ? patternFreq : 1);
        termFreq.put(term, support + termFreq.get(term));
        totalSupport += support;
      }
      ++rank;
    }
    for (String term : termFreq.keys()) {
      float probAfter = termFreq.get(term) / totalSupport;
      
      String fieldname = (pdwFromTwitter ? TweetField.STEMMED_EN.name
          : AssocField.STEMMED_EN.name);
      Term termTerm = new Term(fieldname,
          queryTermFreq(term, null,
              tweetStemmingAnalyzer,
              // tweetNonStemmingAnalyzer,
              fieldname)
              .keys().get(0));
      float probBefore;
      if (pdwFromTwitter) {
        probBefore = twtIxReader.docFreq(termTerm) / TWITTER_CORPUS_LENGTH_IN_TERMS;
      } else {
        probBefore = fisIxReader.docFreq(termTerm) / ITEMSET_CORPUS_LENGTH_IN_TERMS;
      }
      float score = (float) (probAfter * Math.log(probAfter / probBefore));
      if (minScoreOut != null && score < minScoreOut.floatValue()) {
        minScoreOut.setValue(score);
      }
      
      if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
        maxScoreOut.setValue(score);
      }
      
      if (totalScoreOut != null) {
        totalScoreOut.add(score);
      }
    }
    
    return termFreq;
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsConditionalProb(
      OpenIntFloatHashMap fisRs,
      String query, int numItemsetsToUse, // boolean propagateSupport,
      MutableFloat minScoreOut,
      MutableFloat maxScoreOut, MutableFloat totalScoreOut) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    
    if (minScoreOut != null)
      minScoreOut.setValue(Float.MAX_VALUE);
    
    if (maxScoreOut != null)
      maxScoreOut.setValue(Float.MIN_VALUE);
    
    if (totalScoreOut != null)
      totalScoreOut.setValue(0);
    
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    // MutableLong qLen = new MutableLong();
    OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    Set<String> querySet = Sets.newCopyOnWriteArraySet(queryFreq.keys());
    Set<Set<String>> queryPowerSet = Sets.powerSet(querySet);
    OpenObjectFloatHashMap<Set<String>> subsetFreq = new OpenObjectFloatHashMap<Set<String>>();
    OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>();
    float totalW = 0;
    
    LinkedHashMap<Set<String>, Float> rankedItemsetScore = Maps.newLinkedHashMap();
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    int rank = 0;
    for (int i = fisRs.size() - 1; i >= 0; --i) {
      if (numItemsetsToUse > 0 && ++rank > numItemsetsToUse) {
        break;
      }
      
      int hit = keyList.getQuick(i);
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      if (termSet.size() < MIN_ITEMSET_SIZE || rankedItemsetScore.containsKey(termSet)) {
        continue;
      }
      
      float patternFreq = patternPair.getSecond();
      float w = patternFreq; // or 1??
      
      rankedItemsetScore.put(termSet, patternFreq);
      
      boolean weightNotAdded = true;
      for (String term : termSet) {
        
        if (queryFreq.containsKey(term)) {
          continue;
        }
        
        termFreq.put(term, termFreq.get(term) + w);
        for (Set<String> qSub : queryPowerSet) {
          if (qSub.isEmpty()) {
            continue;
          }
          if (!Sets.intersection(termSet, qSub).equals(qSub)) {
            // This query s
            continue;
          } else if (weightNotAdded) {
            subsetFreq.put(qSub, subsetFreq.get(qSub) + w);
          }
          
          Set<String> key = Sets.union(qSub, ImmutableSet.<String> of(term));
          subsetFreq.put(key, subsetFreq.get(key) + w);
          totalW += w;
        }
        weightNotAdded = false;
      }
    }
    
    subsetFreq.put(ImmutableSet.<String> of(), totalW);
    for (String t : termFreq.keys()) {
      if (queryFreq.containsKey(t)) {
        continue;
      }
      
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
        
        List<String> qtPair = ImmutableList.<String> of(t, qTerm);
        termQueryQualityAggr += queryFreq.get(qTerm) * aggregateTermQueryQuality(termQueryQuality,
            Sets.newCopyOnWriteArraySet(qtPair),
            termQueryQuality.get(tAsSet));
      }
      if (termQueryQualityAggr >= 1) {
        // FIXME: This happens in case of repeated hashtag
        termQueryQualityAggr = (float) (1 - 1E-6);
      }
      
      // Log odds
      // termQueryQualityAggr /= (1 - termQueryQualityAggr);
      // termQueryQualityAggr = (float) Math.log(termQueryQualityAggr);
      
      // ////////////////////
      float termCorpusQuality = 0;
      if (twitterCorpusModelWeight > 0) {
        // Collection metric // TODO: use stemmed corpus metrics
        Term tTerm = new Term(TweetField.TEXT.name, t);
        float docFreqC = twtIxReader.docFreq(tTerm);
        if (docFreqC == 0) {
          continue;
        }
        
        termCorpusQuality = (termWeightSmoother + docFreqC) / TWITTER_CORPUS_LENGTH_IN_TERMS;
        
        // // odds of the term (or log odds)
        // termCorpusQuality = (termWeightSmoother + docFreqC) / twtIxReader.numDocs();
        // termCorpusQuality = termCorpusQuality / (1 - termCorpusQuality);
        // termCorpusQuality = (float) Math.log(termCorpusQuality);
        //
        // // IDF is has very large scale compared to probabilities
        // // float termCorpusQuality = (float) (Math.log(twtIxReader.numDocs() / (float) (docFreqC
        // +
        // // 1)) + 1.0);
      }
      
      float pTermSmoothed = (float) (twitterCorpusModelWeight * termCorpusQuality) +
          ((1 - twitterCorpusModelWeight) * (termFreq.get(t) / totalW));
      
      // Should divide by probability of query, but this is a fixed value for all terms
      float score = pTermSmoothed * termQueryQualityAggr;
      
      result.put(t, score);
      
      if (minScoreOut != null && score < minScoreOut.floatValue()) {
        minScoreOut.setValue(score);
      }
      
      if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
        maxScoreOut.setValue(score);
      }
      
      if (totalScoreOut != null) {
        totalScoreOut.add(score);
      }
    }
    
    return result;
  }
  
  // ////////////////////// PATTERN-TO-TERM ///////////////////////////////
  
  public OpenObjectFloatHashMap<String> weightedTermsMarkovChain(
      OpenIntFloatHashMap fisRs,
      String query,
      int numItemsetsToUse, // boolean propagateSupport,
      MutableFloat minScoreOut,
      MutableFloat maxScoreOut, MutableFloat totalScoreOut, int numWalkSteps, double alpha,
      boolean pdwFromTwitter)
      throws IOException,
      org.apache.lucene.queryParser.ParseException {
    
    if (numItemsetsToUse <= 0) {
      throw new IllegalArgumentException("Must specify number of itemsets");
    }
    
    if (minScoreOut != null)
      minScoreOut.setValue(Float.MAX_VALUE);
    
    if (maxScoreOut != null)
      maxScoreOut.setValue(Float.MIN_VALUE);
    
    if (totalScoreOut != null)
      totalScoreOut.setValue(0);
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    
    // LinkedHashMap<IntArrayList, Float> patternSupportMap = new LinkedHashMap<IntArrayList,
    // Float>();
    LinkedHashMap<Set<String>, Float> itemsetsMap = new LinkedHashMap<Set<String>, Float>();
    // List<String> idTermMap = Lists.newArrayList();
    List<Term> stemmedList = Lists.newArrayList();
    
    OpenIntFloatHashMap[] aPDWTemp = new OpenIntFloatHashMap[numItemsetsToUse];
    List<OpenIntFloatHashMap> bPWDTemp = Lists.newArrayList();
    
    int rank = -1;
    int nextId = 0;
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    
    for (int i = fisRs.size() - 1; i >= 0; --i) {
      if (++rank >= numItemsetsToUse) {
        break;
      }
      aPDWTemp[rank] = new OpenIntFloatHashMap();
      
      int hit = keyList.getQuick(i);
      
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      
      if (termSet.size() < MIN_ITEMSET_SIZE || itemsetsMap.containsKey(termSet)) {
        continue;
      }
      
      itemsetsMap.put(termSet, patternPair.getSecond());
      
      // IntArrayList pattern = new IntArrayList(termSet.size());
      
      for (String term : termSet) {
        int termId;
        if (!termIdMap.containsKey(term)) {
          termId = nextId++;
          termIdMap.put(term, termId);
          // idTermMap.add(term);
          bPWDTemp.add(new OpenIntFloatHashMap());
          String fieldname = (pdwFromTwitter ? TweetField.STEMMED_EN.name
              : AssocField.STEMMED_EN.name);
          stemmedList.add(new Term(fieldname,
              queryTermFreq(term, null, tweetStemmingAnalyzer, fieldname).keys().get(0)));
        }
        termId = termIdMap.get(term);
        
        Term stemmed = stemmedList.get(termId);
        
        // /// HEY THESE ARE MY OWN APPROXIMATIONSS >>> HOPE FOR THE BESTT!!!!!
        float totalLength;
        if (pdwFromTwitter) {
          totalLength = twtIxReader.docFreq(stemmed) * BM25Collector.LAVG;
        } else {
          totalLength = fisIxReader.docFreq(stemmed) * FISCollector.AVG_PATTERN_LENGTH;
        }
        float pdw;
        if (totalLength == 0) {
          pdw = 0;
        } else {
          pdw = (termSet.size() * patternPair.getSecond()) / (totalLength);
        }
        float pwd = 1.0f / termSet.size();
        
        aPDWTemp[rank].put(termId, pdw);
        bPWDTemp.get(termId).put(rank, pwd);
        
        // pattern.add(termId);
      }
      
      // patternSupportMap.put(pattern, patternPair.getSecond());
    }
    
    if (bPWDTemp.size() == 0) {
      return new OpenObjectFloatHashMap<String>();
    }
    // In case rank is less
    numItemsetsToUse = rank;
    
    // There's a transpose happening while copying
    double[][] aPDW = new double[bPWDTemp.size()][numItemsetsToUse];
    double[][] bPWD = new double[numItemsetsToUse][bPWDTemp.size()];
    for (int d = 0; d < numItemsetsToUse; ++d) {
      for (int w = 0; w < bPWDTemp.size(); ++w) {
        aPDW[w][d] = aPDWTemp[d].get(w);
        bPWD[d][w] = bPWDTemp.get(w).get(d);
      }
    }
    
    Matrix A = new Matrix(aPDW); // Probability of Document given word
    Matrix B = new Matrix(bPWD); // Probability of Word given document
    Matrix C = A.times(B);
    
    Matrix tQW = Matrix.identity(bPWDTemp.size(), bPWDTemp.size()); // numItemsetsToUse);
    if (numWalkSteps == 1) {
      tQW = tQW.minus(C.times(alpha));
      tQW = tQW.inverse();
    } else {
      for (int k = 0; k < numWalkSteps - 1; ++k) {
        tQW = tQW.plus(C.times(alpha));
        alpha *= alpha;
        C = C.times(C);
      }
      tQW = tQW.plus(C.times(alpha));
    }
    Matrix tDW = tQW.times(A);
    
    tDW = tDW.times(1.0 - alpha);
    tQW = tQW.times(1.0 - alpha);
    
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    // MutableLong qLen = new MutableLong();
    OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    Set<String> querySet = Sets.newCopyOnWriteArraySet(queryFreq.keys());
    
    // probabibilty of query given document
    float[] pqd = new float[numItemsetsToUse];
    int d = 0;
    for (Set<String> termSet : itemsetsMap.keySet()) {
      pqd[d] = 1;
      for (String qTerm : querySet) {
        if (!termSet.contains(qTerm)) {
          continue;
        }
        int qTermId = termIdMap.get(qTerm);
        double qTermProb = 0;
        for (String dTerm : termSet) {
          int dTermId = termIdMap.get(dTerm);
          qTermProb += tQW.get(dTermId, qTermId) * bPWD[d][dTermId];
        }
        if (qTermProb != 0) {
          pqd[d] *= qTermProb;
        }
        // else {
        // LOG.trace("breakpoint");
        // }
      }
      if (pqd[d] == 1) {
        pqd[d] = 0;
      }
      ++d;
    }
    
    d = 0;
    for (Set<String> termSet : itemsetsMap.keySet()) {
      for (String term : termSet) {
        if (querySet.contains(term)) {
          continue;
        }
        
        int termId = termIdMap.get(term);
        
        double docScore = pqd[d] * aPDW[termId][d];
        result.put(term, result.get(term) + (float) docScore);
      }
      ++d;
    }
    
    for (String term : result.keys()) {
      float score = result.get(term);
      
      if (score > 0) {
        int termId = termIdMap.get(term);
        // TODO Smoothing revise
        if (pdwFromTwitter) {
          score *= (twtIxReader.docFreq(stemmedList.get(termId)) + termWeightSmoother)
              / (TWITTER_CORPUS_LENGTH_IN_TERMS + termWeightSmoother);
        } else {
          score *= fisIxReader.docFreq(stemmedList.get(termId)) + termWeightSmoother
              / (ITEMSET_CORPUS_LENGTH_IN_TERMS + termWeightSmoother);
        }
        result.put(term, score);
      } else {
        result.removeKey(term);
      }
      
      if (minScoreOut != null && score < minScoreOut.floatValue()) {
        minScoreOut.setValue(score);
      }
      
      if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
        maxScoreOut.setValue(score);
      }
      
      if (totalScoreOut != null) {
        totalScoreOut.add(score);
      }
      
    }
    
    return result;
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsSVDOfPatternMatrix(
      OpenIntFloatHashMap fisRs,
      String query,
      int numItemsetsToUse, // boolean propagateSupport,
      MutableFloat minScoreOut,
      MutableFloat maxScoreOut, MutableFloat totalScoreOut,
      boolean pdwFromTwitter)
      throws IOException,
      org.apache.lucene.queryParser.ParseException {
    
    if (numItemsetsToUse <= 0) {
      throw new IllegalArgumentException("Must specify number of itemsets");
    }
    
    if (minScoreOut != null)
      minScoreOut.setValue(Float.MAX_VALUE);
    
    if (maxScoreOut != null)
      maxScoreOut.setValue(Float.MIN_VALUE);
    
    if (totalScoreOut != null)
      totalScoreOut.setValue(0);
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    OpenObjectFloatHashMap<String> termFISFreq = new OpenObjectFloatHashMap<String>();
    float totalSupport = 0;
    // LinkedHashMap<IntArrayList, Float> patternSupportMap = new LinkedHashMap<IntArrayList,
    // Float>();
    LinkedHashMap<Set<String>, Float> itemsetsMap = new LinkedHashMap<Set<String>, Float>();
    List<String> idTermMap = Lists.newArrayList();
    List<Term> stemmedList = Lists.newArrayList();
    
    List<OpenIntFloatHashMap> bWordDocTemp = Lists.newArrayList();
    
    int rank = -1;
    int nextId = 0;
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    
    for (int i = fisRs.size() - 1; i >= 0; --i) {
      if (++rank >= numItemsetsToUse) {
        break;
      }
      
      int hit = keyList.getQuick(i);
      
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      
      if (termSet.size() < MIN_ITEMSET_SIZE || itemsetsMap.containsKey(termSet)) {
        continue;
      }
      
      itemsetsMap.put(termSet, patternPair.getSecond());
      
      // IntArrayList pattern = new IntArrayList(termSet.size());
      
      for (String term : termSet) {
        int termId;
        if (!termIdMap.containsKey(term)) {
          termId = nextId++;
          termIdMap.put(term, termId);
          idTermMap.add(term);
          bWordDocTemp.add(new OpenIntFloatHashMap());
          String fieldname = (pdwFromTwitter ? TweetField.STEMMED_EN.name
              : AssocField.STEMMED_EN.name);
          stemmedList.add(new Term(fieldname,
              queryTermFreq(term, null, tweetStemmingAnalyzer, fieldname).keys().get(0)));
        }
        termId = termIdMap.get(term);
        
        Term stemmed = stemmedList.get(termId);
        
        // /// HEY THESE ARE MY OWN APPROXIMATIONSS >>> HOPE FOR THE BESTT!!!!!
        float globalTi;
        // TODO revise smoothing
        if (pdwFromTwitter) {
          globalTi = (termWeightSmoother + twtIxReader.docFreq(stemmed))
              / (termWeightSmoother + TWITTER_CORPUS_LENGTH_IN_TERMS);
        } else {
          globalTi = (termWeightSmoother + fisIxReader.docFreq(stemmed))
              / (termWeightSmoother + ITEMSET_CORPUS_LENGTH_IN_TERMS);
        }
        
        bWordDocTemp.get(termId).put(rank, globalTi);
        
        termFISFreq.put(term, termFISFreq.get(term) + patternPair.getSecond());
        totalSupport += patternPair.getSecond();
        // pattern.add(termId);
      }
      
      // patternSupportMap.put(pattern, patternPair.getSecond());
    }
    
    if (bWordDocTemp.size() == 0) {
      return new OpenObjectFloatHashMap<String>();
    }
    // In case rank is less
    numItemsetsToUse = rank;
    
    SummaryStatistics[] colStats = new SummaryStatistics[numItemsetsToUse];
    double[][] bPWD = new double[bWordDocTemp.size()][numItemsetsToUse];
    for (int d = 0; d < numItemsetsToUse; ++d) {
      colStats[d] = new SummaryStatistics();
      for (int w = 0; w < bWordDocTemp.size(); ++w) {
        bPWD[w][d] = bWordDocTemp.get(w).get(d);
        float fisIDF = termFISFreq.get(idTermMap.get(w)) / totalSupport;
        float corpusIDF;
        if (pdwFromTwitter) {
          corpusIDF = 1.0f * twtIxReader.docFreq(stemmedList.get(w)) / twtIxReader.numDocs();
        } else {
          corpusIDF = 1.0f * fisIxReader.docFreq(stemmedList.get(w)) / fisIxReader.numDocs();
        }
        if (corpusIDF == 0) {
          // shit happens!!!
          bPWD[w][d] = 0;
        } else {
          bPWD[w][d] *= Math.log(fisIDF / corpusIDF);
        }
        colStats[d].addValue(bPWD[w][d]);
      }
    }
    
    for (int d = 0; d < numItemsetsToUse; ++d) {
      for (int w = 0; w < bWordDocTemp.size(); ++w) {
        bPWD[w][d] -= colStats[d].getMean(); // TODO divide by standard deviation??
      }
    }
    
    Matrix B = new Matrix(bPWD);
    SingularValueDecomposition svd = new SingularValueDecomposition(B);
    double[] termWeights = svd.getSingularValues();
    
    OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    for (int w = 0; w < termWeights.length; ++w) {
      if (termWeights[w] == 0) {
        continue;
      }
      String term = idTermMap.get(w);
      if (queryFreq.containsKey(term)) {
        continue;
      }
      result.put(term, (float) termWeights[w]);
    }
    
    return result;
  }
  
  public PriorityQueue<ScoreIxObj<String>>[] weightedTermsClusterPatterns(
      OpenIntFloatHashMap fisRs,
      String query,
      int numItemsetsToUse, // boolean propagateSupport,
      List<MutableFloat> minXTermScoresOut, List<MutableFloat> maxXTermScoresOut,
      List<MutableFloat> totalXTermScoresOut,
      boolean pdwFromTwitter, boolean oneMinusInTheEnd)
      throws Exception {
    
    if (numItemsetsToUse <= 0) {
      throw new IllegalArgumentException("Must specify number of itemsets");
    }
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    OpenObjectFloatHashMap<String> termFISFreq = new OpenObjectFloatHashMap<String>();
    float totalSupport = 0;
    // LinkedHashMap<IntArrayList, Float> patternSupportMap = new LinkedHashMap<IntArrayList,
    // Float>();
    LinkedHashMap<Set<String>, Float> itemsetsMap = new LinkedHashMap<Set<String>, Float>();
    List<String> idTermMap = Lists.newArrayList();
    List<Term> stemmedList = Lists.newArrayList();
    FastVector attrs = new FastVector();
    
    List<OpenIntFloatHashMap> bWordDocTemp = Lists.newArrayList();
    
    int rank = -1;
    int nextId = 0;
    
    IntArrayList keyList = new IntArrayList(fisRs.size());
    fisRs.keysSortedByValue(keyList);
    
    for (int i = fisRs.size() - 1; i >= 0; --i) {
      if (++rank >= numItemsetsToUse) {
        break;
      }
      
      int hit = keyList.getQuick(i);
      
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      
      if (termSet.size() < MIN_ITEMSET_SIZE || itemsetsMap.containsKey(termSet)) {
        continue;
      }
      
      itemsetsMap.put(termSet, patternPair.getSecond());
      
      // IntArrayList pattern = new IntArrayList(termSet.size());
      
      for (String term : termSet) {
        int termId;
        if (!termIdMap.containsKey(term)) {
          termId = nextId++;
          termIdMap.put(term, termId);
          idTermMap.add(term);
          bWordDocTemp.add(new OpenIntFloatHashMap());
          String fieldname = (pdwFromTwitter ? TweetField.STEMMED_EN.name
              : AssocField.STEMMED_EN.name);
          stemmedList.add(new Term(fieldname,
              queryTermFreq(term, null, tweetStemmingAnalyzer, fieldname).keys().get(0)));
          attrs.addElement(new Attribute(term));
        }
        termId = termIdMap.get(term);
        
        Term stemmed = stemmedList.get(termId);
        
        // /// HEY THESE ARE MY OWN APPROXIMATIONSS >>> HOPE FOR THE BESTT!!!!!
        float globalTi;
        // TODO revise smoothing
        if (pdwFromTwitter) {
          globalTi = (termWeightSmoother + twtIxReader.docFreq(stemmed))
              / (termWeightSmoother + TWITTER_CORPUS_LENGTH_IN_TERMS);
        } else {
          globalTi = (termWeightSmoother + fisIxReader.docFreq(stemmed))
              / (termWeightSmoother + ITEMSET_CORPUS_LENGTH_IN_TERMS);
        }
        
        bWordDocTemp.get(termId).put(rank, globalTi);
        
        termFISFreq.put(term, termFISFreq.get(term) + patternPair.getSecond());
        totalSupport += patternPair.getSecond();
        // pattern.add(termId);
      }
      
      // patternSupportMap.put(pattern, patternPair.getSecond());
    }
    
    if (bWordDocTemp.size() == 0) {
      return new PriorityQueue[0];
    }
    // In case rank is less
    numItemsetsToUse = rank;
    
    SummaryStatistics[] colStats = new SummaryStatistics[numItemsetsToUse];
    
    double[][] bPWD = new double[bWordDocTemp.size()][numItemsetsToUse];
    for (int d = 0; d < numItemsetsToUse; ++d) {
      colStats[d] = new SummaryStatistics();
      for (int w = 0; w < bWordDocTemp.size(); ++w) {
        bPWD[w][d] = bWordDocTemp.get(w).get(d);
        float fisIDF = termFISFreq.get(idTermMap.get(w)) / totalSupport;
        float corpusIDF;
        if (pdwFromTwitter) {
          corpusIDF = 1.0f * twtIxReader.docFreq(stemmedList.get(w)) / twtIxReader.numDocs();
        } else {
          corpusIDF = 1.0f * fisIxReader.docFreq(stemmedList.get(w)) / fisIxReader.numDocs();
        }
        if (corpusIDF == 0) {
          // shit happens!!!
          bPWD[w][d] = 0;
        } else {
          bPWD[w][d] *= Math.log(fisIDF / corpusIDF);
        }
        colStats[d].addValue(bPWD[w][d]);
      }
    }
    Instances insts = new Instances("pattern-term", attrs, numItemsetsToUse);
    for (int d = 0; d < numItemsetsToUse; ++d) {
      Instance instance = new Instance(attrs.size());
      for (int w = 0; w < bWordDocTemp.size(); ++w) {
        instance.setValue(w, bPWD[w][d] - colStats[d].getMean()); // TODO divide by standard
                                                                  // deviation??
      }
      
      insts.add(instance);
    }
    if (insts == null || insts.numInstances() == 0) {
      return new PriorityQueue[0];
    }
    
    XMeans clusterer = new XMeans();
    clusterer.setDistanceF(new CosineDistance());
    clusterer.buildClusterer(insts);
    
    OpenObjectFloatHashMap<String>[] termWeights = new OpenObjectFloatHashMap[clusterer
        .numberOfClusters()];
    for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
      termWeights[c] = new OpenObjectFloatHashMap<String>();
      if (minXTermScoresOut != null)
        minXTermScoresOut.add(new MutableFloat(Float.MAX_VALUE));
      
      if (maxXTermScoresOut != null)
        maxXTermScoresOut.add(new MutableFloat(Float.MIN_VALUE));
      
      if (totalXTermScoresOut != null)
        totalXTermScoresOut.add(new MutableFloat(0));
    }
    // float maxDistance = Float.MIN_VALUE;
    int d = -1;
    for (Set<String> patternItems : itemsetsMap.keySet()) {
      ++d;
      Instance inst = insts.instance(d);
      for (String item : patternItems) {
        int termId = termIdMap.get(item);
        String term = ((Attribute) attrs.elementAt(termId)).name();
        double[] distrib = clusterer.distributionForInstance(inst);
        int clusterMembershipCount = 0;
        for (int c = 0; c < distrib.length; ++c) {
          if (distrib[c] <= CLUSTER_MEMBERSHIP_THRESHOLD) {
            continue;
          }
          ++clusterMembershipCount;
          
          // Closeness to centroid
          Instance centroid = clusterer.getClusterCenters().instance(c);
          float score;
          if (oneMinusInTheEnd) {
            // // "1 -" is the last thing to do.. sucker!!!!!
            score = (float) clusterer.getDistanceF().distance(centroid, inst);
          } else {
            score = 1 - (float) clusterer.getDistanceF().distance(centroid, inst);
          }
          
          score += termWeights[c].get(term);
          
          // if (maxDistance < score) {
          // maxDistance = score;
          // }
          
          termWeights[c].put(term, score);
          
        }
        // Don't penalize words that pertain to more than one topic.. if not oneMinus, this would
        // actually result in a higher score. Anyway I "think" these should be very few TODO: verify
        if (clusterMembershipCount > 0 && oneMinusInTheEnd) {
          for (int c = 0; c < distrib.length; ++c) {
            termWeights[c].put(term, termWeights[c].get(term) / clusterMembershipCount);
          }
        }
      }
    }
    
    PriorityQueue<ScoreIxObj<String>>[] result = new PriorityQueue[clusterer.numberOfClusters()];
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query, null,
        (FISQueryExpander.SEARCH_NON_STEMMED ? FISQueryExpander.tweetNonStemmingAnalyzer
            : FISQueryExpander.tweetStemmingAnalyzer),
        (FISQueryExpander.SEARCH_NON_STEMMED ? TweetField.TEXT.name : TweetField.STEMMED_EN.name));
    for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
      result[c] = new PriorityQueue<ScoreIxObj<String>>();
      for (String term : termWeights[c].keys()) {
        float score = termWeights[c].get(term);
        
        if (queryTerms.containsKey(term) || score == 0) {
          continue;
        }
        
        if (oneMinusInTheEnd) {
          score = 1 - score;
        }
        if (minXTermScoresOut != null) {
          if (score < minXTermScoresOut.get(c).floatValue()) {
            minXTermScoresOut.get(c).setValue(score);
          }
        }
        
        if (maxXTermScoresOut != null) {
          if (score > maxXTermScoresOut.get(c).floatValue()) {
            maxXTermScoresOut.get(c).setValue(score);
          }
        }
        
        if (totalXTermScoresOut != null) {
          totalXTermScoresOut.get(c).add(score);
        }
        
        result[c].add(new ScoreIxObj<String>(term, score));
      }
    }
    
    return result;
  }
  
  @SuppressWarnings("unchecked")
  public OpenObjectFloatHashMap<String> weightedTermsByPageRankPatterns(
      OpenIntFloatHashMap rs, String query, int numItemsetsToConsider,
      MutableFloat minXTermScoresOut, MutableFloat maxXTermScoresOut,
      MutableFloat totalXTermScoresOut, int nFromTopPatterns, float damping, int maxIters,
      float minDelta) throws Exception {
    
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    Map<IntArrayList, Instance> patternInstMap = Maps.newLinkedHashMap();
    LinkedHashSet<Set<String>> itemsets = Sets.newLinkedHashSet();
    FastVector attrs = new FastVector();
    Instances insts = createPatternTermMatrix(rs,
        true,
        attrs,
        patternInstMap,
        true,
        itemsets, false);
    if (insts == null || insts.numInstances() == 0) {
      return result;
    }
    
    OpenObjectFloatHashMap<PairOfInts> edges = new OpenObjectFloatHashMap<PairOfInts>();
    int[] out = new int[insts.numInstances()];
    // TODO sinks??
    for (int i = 0; i < insts.numInstances(); ++i) {
      Instance insti = insts.instance(i);
      for (int j = 0; j < insts.numAttributes(); ++j) {
        if (insti.isMissing(j) || insti.value(j) == 0) {
          continue;
        }
        double[] sharedTerms = insts.attributeToDoubleArray(j);
        for (int k = 0; k < sharedTerms.length; ++k) {
          if (i == k || sharedTerms[k] == 0 || Double.isNaN(sharedTerms[k])) {
            continue;
          }
          ++out[i];
          edges.put(new PairOfInts(i, k), (float) insti.weight());
        }
      }
    }
    OpenIntFloatHashMap rank = pageRank(edges,
        out,
        insts.numInstances(),
        damping,
        maxIters,
        minDelta);
    
    if (minXTermScoresOut != null) {
      minXTermScoresOut.setValue(Float.MAX_VALUE);
    }
    if (maxXTermScoresOut != null) {
      maxXTermScoresOut.setValue(Float.MIN_VALUE);
    }
    if (totalXTermScoresOut != null) {
      totalXTermScoresOut.setValue(0);
    }
    
    ArrayList<Set<String>> itemSetArr = Lists.newArrayList(itemsets);
    IntArrayList rankedIx = new IntArrayList();
    rank.keysSortedByValue(rankedIx);
    int n = 0;
    for (int i = rankedIx.size() - 1; i >= 0; --i) {
      Set<String> fis = itemSetArr.get(i);
      float s = rank.get(i);
      for (String t : fis) {
        float score;
        if (nFromTopPatterns > 0) {
          if (result.containsKey(t)) {
            continue;
          }
          score = s;
          result.put(t, s);
          if (++n == nFromTopPatterns) {
            break;
          }
        } else {
          score = result.get(t) + s;
          result.put(t, score);
        }
        
        if (minXTermScoresOut != null && minXTermScoresOut.floatValue() > score) {
          minXTermScoresOut.setValue(score);
        }
        if (maxXTermScoresOut != null && maxXTermScoresOut.floatValue() < score) {
          maxXTermScoresOut.setValue(score);
        }
        if (totalXTermScoresOut != null) {
          totalXTermScoresOut.add(score);
        }
      }
    }
    return result;
  }
  
  public static OpenIntFloatHashMap pageRank(OpenObjectFloatHashMap<PairOfInts> edges,
      int[] out, int numPages, float damping, int maxIters, float minDelta) {
    
    OpenIntFloatHashMap r1 = new OpenIntFloatHashMap(numPages);
    for (int i = 0; i < numPages; ++i) {
      r1.put(i, 1);
    }
    float maxDelta = Float.MIN_VALUE;
    
    int iters = 0;
    while (iters++ < maxIters) {
      float[] r2 = new float[numPages];
      for (int i = 0; i < r2.length; ++i) {
        r2[i] = (1 - damping);
      }
      for (PairOfInts e : edges.keys()) {
        int i = e.getLeftElement();
        int j = e.getRightElement();
        r2[j] += (damping / out[i]) * r1.get(i) * edges.get(e);
      }
      float s = numPages;
      for (int i = 0; i < numPages; ++i) {
        s -= r2[i];
      }
      for (int i = 0; i < numPages; ++i) {
        float delta = r1.get(i);
        r1.put(i, r2[i] + s / numPages);
        delta = r1.get(i) - delta;
        if (delta > maxDelta) {
          maxDelta = delta;
        }
      }
      if (maxDelta < minDelta) {
        break;
      }
    }
    return r1;
  }
  
  // ////////////////////// TERM-TO-TERM ALL TERMS FEATURES //////////////////////////////
  
  protected Instances termCooccurrenceInstances(AbstractIntFloatMap rs,
      int numItemsetsToConsider,
      OpenObjectIntHashMap<String> termIdMapOut,
      LinkedHashMap<Set<String>, Float> itemsetsOut,
      OpenIntFloatHashMap termSupportOut,
      MutableFloat totalSupport, boolean weightInstances) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    int nextId = 0;
    FastVector attrsOut = new FastVector();
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    int rank = 0;
    for (int i = rs.size() - 1; i >= 0; --i) {
      if (numItemsetsToConsider > 0 && ++rank > numItemsetsToConsider) {
        break;
      }
      int hit = keyList.getQuick(i);
      Pair<Set<String>, Float> patternPair = getPattern(hit);
      Set<String> termSet = patternPair.getFirst();
      if (termSet.size() < MIN_ITEMSET_SIZE || itemsetsOut.containsKey(termSet)) {
        continue;
      }
      
      itemsetsOut.put(termSet, patternPair.getSecond());
      
      // IntArrayList pattern = new IntArrayList(termSet.size());
      
      for (String term : termSet) {
        int termId;
        if (!termIdMapOut.containsKey(term)) {
          termIdMapOut.put(term, nextId++);
          attrsOut.addElement(new Attribute(term));
        }
        termId = termIdMapOut.get(term);
        termSupportOut.put(termId, termSupportOut.get(termId) + patternPair.getSecond());
        totalSupport.add(patternPair.getSecond());
        // pattern.add(termIdMapOut.get(term));
      }
      
      // patternTreeOut.addPattern(pattern, 1);
    }
    if (itemsetsOut.isEmpty()) {
      return null;
    }
    
    Instances insts = new Instances("term-term", attrsOut, attrsOut.size());
    
    Enumeration attrsEnum = attrsOut.elements();
    while (attrsEnum.hasMoreElements()) {
      Attribute termAttr = (Attribute) attrsEnum.nextElement();
      double weight = 1;
      if (weightInstances) {
        Term termT = new Term(TweetField.TEXT.name, termAttr.name());
        weight = twtSimilarity.idf(twtIxReader.docFreq(termT),
            twtIxReader.numDocs());
      }
      Instance termInst = new Instance(weight, new double[attrsOut.size()]);
      
      termInst.setDataset(insts);
      insts.add(termInst);
    }
    
    for (Set<String> termSet : itemsetsOut.keySet()) {
      for (String termi : termSet) {
        int i = termIdMapOut.get(termi);
        Instance inst = insts.instance(i);
        for (String termj : termSet) {
          int j = termIdMapOut.get(termj);
          if (i == j) {
            inst.setMissing(j);
          } else {
            double cooccur;
            if (inst.isMissing(j)) {
              cooccur = 0;
            } else {
              cooccur = inst.value(j);
            }
            cooccur += itemsetsOut.get(termSet);
            inst.setValue(j, cooccur);
          }
        }
      }
    }
    
    return insts;
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsBySVDOfTermCoocur(OpenIntFloatHashMap rs,
      String query, int numItemsetsToConsider,
      MutableFloat minScoreOut, MutableFloat maxScoreOut,
      MutableFloat totalScoreOut) throws Exception {
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    if (minScoreOut != null)
      minScoreOut.setValue(Float.MAX_VALUE);
    
    if (maxScoreOut != null)
      maxScoreOut.setValue(Float.MIN_VALUE);
    
    if (totalScoreOut != null)
      totalScoreOut.setValue(0);
    
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    LinkedHashMap<Set<String>, Float> itemsets = Maps.newLinkedHashMap();
    // FastVector attrs = new FastVector();
    OpenIntFloatHashMap termSupport = new OpenIntFloatHashMap();
    MutableFloat totalSupportOut = new MutableFloat();
    
    Instances insts = termCooccurrenceInstances(rs, numItemsetsToConsider,
        termIdMap,
        itemsets,
        termSupport,
        totalSupportOut, true);
    float totalSupport = totalSupportOut.floatValue();
    
    double[][] matrix = new double[insts.numInstances()][insts.numAttributes()];
    for (int j = 0; j < insts.numAttributes(); ++j) {
      double[] attrArr = insts.attributeToDoubleArray(j);
      SummaryStatistics stats = new SummaryStatistics();
      for (double val : attrArr) {
        stats.addValue(val);
      }
      for (int i = 0; i < insts.numInstances(); ++i) {
        matrix[i][j] = (attrArr[i] - stats.getMean());
      }
    }
    
    SingularValueDecomposition svd = new SingularValueDecomposition(new Matrix(matrix));
    double[] termWrights = svd.getSingularValues();
    for (String term : termIdMap.keys()) {
      if (queryTerms.containsKey(term)) {
        continue;
      }
      result.put(term, (float) termWrights[termIdMap.get(term)]);
    }
    
    // LatentSemanticAnalysis lsa = new LatentSemanticAnalysis();
    // lsa.setNormalize(true);
    // lsa.buildEvaluator(insts);
    //
    // for (String term : termIdMap.keys()) {
    // if (queryTerms.containsKey(term)) {
    // continue;
    // }
    // result.put(term, (float) lsa.evaluateAttribute(termIdMap.get(term)));
    // }
    
    return result;
  }
  
  public OpenObjectFloatHashMap<String> weightedTermsByConditionalEntropy(OpenIntFloatHashMap rs,
      String query, int numItemsetsToConsider, int numTermsToReturn,
      MutableFloat minScoreOut, MutableFloat maxScoreOut,
      MutableFloat totalScoreOut, boolean startWithEntropy) throws Exception {
    
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    if (minScoreOut != null)
      minScoreOut.setValue(Float.MAX_VALUE);
    
    if (maxScoreOut != null)
      maxScoreOut.setValue(Float.MIN_VALUE);
    
    if (totalScoreOut != null)
      totalScoreOut.setValue(0);
    
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query,
        null,
        tweetNonStemmingAnalyzer,
        TweetField.TEXT.name);
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    LinkedHashMap<Set<String>, Float> itemsets = Maps.newLinkedHashMap();
    // FastVector attrs = new FastVector();
    OpenIntFloatHashMap termSupport = new OpenIntFloatHashMap();
    MutableFloat totalSupportOut = new MutableFloat();
    
    Instances insts = termCooccurrenceInstances(rs, numItemsetsToConsider,
        termIdMap,
        itemsets,
        termSupport,
        totalSupportOut, false);
    float totalSupport = totalSupportOut.floatValue();
    
    OpenObjectFloatHashMap<Set<String>> patternEntropy = new OpenObjectFloatHashMap<Set<String>>(
        itemsets.size());
    if (startWithEntropy) {
      // // Calculate individual entropy
      for (Set<String> termSet : itemsets.keySet()) {
        float entropy = 0;
        for (String term : termSet) {
          int termId = termIdMap.get(term);
          float probTerm = termSupport.get(termId) / totalSupport;
          entropy += (float) (probTerm * Math.log(1.0 / probTerm) / LOG2);
        }
        patternEntropy.put(termSet, entropy);
      }
    } else {
      
      // Calculate mutual infomarion between terms
      for (Set<String> termSet : itemsets.keySet()) {
        float mi = 0;
        for (String termx : termSet) {
          int idx = termIdMap.get(termx);
          float probX = termSupport.get(idx) / totalSupport;
          for (String termy : termSet) {
            int idy = termIdMap.get(termy);
            if (idy == idx) {
              mi = +(float) (probX * Math.log(1.0 / probX) / LOG2);
              continue;
            }
            
            float probY = termSupport.get(idy) / totalSupport;
            
            double cooccur = insts.instance(idx).value(idy);
            
            float probJoint = (float) (cooccur / totalSupport);
            if (probJoint != 0) {
              mi += probJoint * Math.log(probJoint / (probX * probY)) / LOG2;
            }
          }
        }
        
        // // Calculate mutual infomarion with query
        
        // for (String pTerm : termSet) {
        // int pTermId = termIdMap.get(pTerm);
        // float probTermP = termSupport.get(pTermId) / totalSupport;
        // for (String qTerm : queryTerms.keys()) {
        // int qTermId = termIdMap.get(qTerm);
        // if (qTermId == pTermId) {
        // continue;
        // }
        // float cooccur = (float) insts.instance(pTermId).value(qTermId);
        // if (cooccur == 0) {
        // continue;
        // }
        // float probTermQ = termSupport.get(qTermId) / totalSupport;
        // float probJoint = cooccur / totalSupport;
        //
        // mi += (float) (probJoint * Math.log(probJoint / (probTermQ * probTermP)) / LOG2);
        // }
        // }
        patternEntropy.put(termSet, mi);
      }
    }
    int t = 0;
    // Add terms from the top pattern, reduce the entropies and resort
    // (The top pattern would remain top because all others were reduced)
    for (int d = 1; d <= itemsets.size(); ++d) {
      // for(int d=0; d< itemsets.size(); ++d){
      List<Set<String>> patternsByEntropy = Lists.newArrayListWithCapacity(itemsets.size());
      patternEntropy.keysSortedByValue(patternsByEntropy);
      
      int x = patternsByEntropy.size() - d;
      // int x = d;
      Set<String> patternx = patternsByEntropy.get(x); // highest entropy unadded pattern
      
      float score = patternEntropy.get(patternx);
      
      for (String termx : patternx) {
        if (queryTerms.containsKey(termx) || result.containsKey(termx)) {
          continue;
        }
        
        if (minScoreOut != null && score < minScoreOut.floatValue()) {
          minScoreOut.setValue(score);
        }
        
        if (maxScoreOut != null && score > maxScoreOut.floatValue()) {
          maxScoreOut.setValue(score);
        }
        
        if (totalScoreOut != null) {
          totalScoreOut.add(score);
        }
        
        result.put(termx, score);
        
        if (!result.containsKey("#" + termx)) {
          ++t;
        }
        
        if (t >= numTermsToReturn) {
          return result;
        }
      }
      
      for (int y = x - 1; y >= 0; --y) {
        // for(int y=0; y<x; ++y){
        Set<String> patterny = patternsByEntropy.get(y);
        float origEntropy = patternEntropy.get(patterny);
        if (origEntropy == 0) {
          break;
        }
        float mutualEntropy = 0;
        for (String termx : patternx) {
          int idx = termIdMap.get(termx);
          float probX = termSupport.get(idx) / totalSupport;
          for (String termy : patterny) {
            int idy = termIdMap.get(termy);
            if (idy == idx) {
              mutualEntropy = +(float) (probX * Math.log(1.0 / probX) / LOG2);
              continue;
            }
            float probY = termSupport.get(idy) / totalSupport;
            
            double cooccur = insts.instance(idx).value(idy);
            
            float probJoint = (float) (cooccur / totalSupport);
            if (probJoint != 0) {
              mutualEntropy += probJoint * Math.log(probJoint / (probX * probY)) / LOG2;
            }
          }
        }
        
        patternEntropy.put(patterny, origEntropy - mutualEntropy);
      }
    }
    
    return result;
  }
  
  @SuppressWarnings("unchecked")
  public PriorityQueue<ScoreIxObj<String>>[] weightedTermsByClusteringTerms(
      OpenIntFloatHashMap rs, String query, int numItemsetsToConsider,
      List<MutableFloat> minXTermScoresOut, List<MutableFloat> maxXTermScoresOut,
      List<MutableFloat> totalXTermScoresOut) throws Exception {
    
    PriorityQueue<ScoreIxObj<String>>[] result = null;
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    LinkedHashMap<Set<String>, Float> itemsets = Maps.newLinkedHashMap();
    // FastVector attrs = new FastVector();
    OpenIntFloatHashMap termSupport = new OpenIntFloatHashMap();
    MutableFloat totalSupport = new MutableFloat();
    
    Instances insts = termCooccurrenceInstances(rs, numItemsetsToConsider,
        termIdMap,
        itemsets,
        termSupport,
        totalSupport, true);
    
    // if (itemsets.isEmpty()) {
    // return result;
    // }
    if (insts == null || insts.numInstances() == 0) {
      return new PriorityQueue[0];
    }
    
    LatentSemanticAnalysis lsa = new LatentSemanticAnalysis();
    if (PARAM_CLUSTERING_APPLY_LSA) {
      lsa.setNormalize(true);
      lsa.buildEvaluator(insts);
      insts = lsa.transformedData(insts);
    }
    // HMMMM????
    // Normalize norm = new Normalize();
    // norm.setInputFormat(insts);
    // insts = Filter.useFilter(insts, norm);
    // Standardize standardize = new S
    
    XMeans clusterer = new XMeans();
    ((XMeans) clusterer).setDistanceF(new CosineDistance());
    clusterer.buildClusterer(insts);
    
    LOG.info("Number of clusters: {}", clusterer.numberOfClusters());
    
    result = new PriorityQueue[clusterer.numberOfClusters()];
    if (TERM_SCORE_PERCLUSTER) {
      for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
        result[c] = new PriorityQueue<ScoreIxObj<String>>();
        if (minXTermScoresOut != null) {
          minXTermScoresOut.add(new MutableFloat(Float.MAX_VALUE));
        }
        if (maxXTermScoresOut != null) {
          maxXTermScoresOut.add(new MutableFloat(Float.MIN_VALUE));
        }
        if (totalXTermScoresOut != null) {
          totalXTermScoresOut.add(new MutableFloat(0));
        }
      }
      
      for (String term : termIdMap.keys()) {
        int termId = termIdMap.get(term);
        Instance inst = insts.instance(termId);
        // if (PARAM_CLUSTERING_APPLY_LSA) {
        // inst = lsa.convertInstance(inst);
        // }
        double[] distrib = clusterer.distributionForInstance(inst);
        for (int c = 0; c < distrib.length; ++c) {
          if (distrib[c] <= CLUSTER_MEMBERSHIP_THRESHOLD) {
            continue;
          }
          
          // Probability is meaningless as score because it comes as either 0 or 1
          // float score = termDistrib[c];
          
          // Closeness to centroid
          Instance centroid = clusterer.getClusterCenters().instance(c);
          float score = 1 - (float) clusterer.getDistanceF().distance(centroid, inst);
          
          result[c].add(new ScoreIxObj<String>(term, score));
          
          if (minXTermScoresOut != null) {
            if (score < minXTermScoresOut.get(c).floatValue()) {
              minXTermScoresOut.get(c).setValue(score);
            }
          }
          
          if (maxXTermScoresOut != null) {
            if (score > maxXTermScoresOut.get(c).floatValue()) {
              maxXTermScoresOut.get(c).setValue(score);
            }
          }
          
          if (totalXTermScoresOut != null) {
            totalXTermScoresOut.get(c).add(score);
          }
        }
      }
      
    } else {
      throw new UnsupportedOperationException("TODO: change to avoid using the obsolete convertXX");
      // MutableFloat minScore = new MutableFloat(Float.MAX_VALUE);
      // MutableFloat maxScore = new MutableFloat(Float.MIN_VALUE);
      // MutableFloat totalScore = new MutableFloat(0);
      // PriorityQueue<ScoreIxObj<String>> termScores = convertResultToWeightedTermsKLDivergence(rs,
      // query,
      // -1,
      // false,
      // minScore,
      // maxScore,
      // totalScore);
      // // convertResultToWeightedTermsConditionalProb(rs,
      // // query,
      // // -1,
      // // false,
      // // minScore,
      // // maxScore,
      // // totalScore);
      //
      // for (int c = 0; c < result.length; ++c) {
      // result[c] = new PriorityQueue<ScoreIxObj<String>>();
      // if (minXTermScoresOut != null) {
      // minXTermScoresOut.add(minScore);
      // }
      //
      // if (maxXTermScoresOut != null) {
      // maxXTermScoresOut.add(maxScore);
      // }
      //
      // if (totalXTermScoresOut != null) {
      // totalXTermScoresOut.add(totalScore);
      // }
      //
      // PriorityQueue<ScoreIxObj<String>> termScoresClone = new
      // PriorityQueue<ScoreIxObj<String>>();
      // while (!termScores.isEmpty()) {
      // ScoreIxObj<String> scoredTerm = termScores.poll();
      // termScoresClone.add(scoredTerm);
      // int termId = termIdMap.get(scoredTerm.obj);
      // Instance inst = insts.instance(termId);
      // // if (PARAM_CLUSTERING_APPLY_LSA) {
      // // inst = lsa.convertInstance(inst);
      // // }
      // double[] distrib = clusterer.distributionForInstance(inst);
      // if (distrib[c] > CLUSTER_MEMBERSHIP_THRESHOLD) {
      // result[c].add(scoredTerm);
      // }
      // }
      // termScores = termScoresClone;
      //
      // }
    }
    return result;
  }
  
  @SuppressWarnings("unchecked")
  public OpenObjectFloatHashMap<String> weightedTermsByPageRankTerms(
      OpenIntFloatHashMap rs, String query, int numItemsetsToConsider,
      MutableFloat minXTermScoresOut, MutableFloat maxXTermScoresOut,
      MutableFloat totalXTermScoresOut, float damping, int maxIters, float minDelta)
      throws Exception {
    
    OpenObjectFloatHashMap<String> result = new OpenObjectFloatHashMap<String>();
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    LinkedHashMap<Set<String>, Float> itemsets = Maps.newLinkedHashMap();
    // FastVector attrs = new FastVector();
    OpenIntFloatHashMap termSupport = new OpenIntFloatHashMap();
    MutableFloat totalSupport = new MutableFloat();
    
    Instances insts = termCooccurrenceInstances(rs, numItemsetsToConsider,
        termIdMap,
        itemsets,
        termSupport,
        totalSupport, true);
    
    OpenObjectFloatHashMap<PairOfInts> edges = new OpenObjectFloatHashMap<PairOfInts>();
    int[] out = new int[insts.numInstances()];
    // TODO sinks??
    for (int i = 0; i < insts.numInstances(); ++i) {
      Instance inst = insts.instance(i);
      boolean isSink = true;
      for (int j = 0; j < insts.numAttributes(); ++j) {
        
        if (inst.isMissing(j)) {
          continue;
        }
        ++out[i];
        isSink = false;
        edges.put(new PairOfInts(i, j), (float) inst.value(j));
      }
    }
    
    OpenIntFloatHashMap rank = pageRank(edges,
        out,
        insts.numInstances(),
        damping,
        maxIters,
        minDelta);
    
    if (minXTermScoresOut != null) {
      minXTermScoresOut.setValue(Float.MAX_VALUE);
    }
    if (maxXTermScoresOut != null) {
      maxXTermScoresOut.setValue(Float.MIN_VALUE);
    }
    if (totalXTermScoresOut != null) {
      totalXTermScoresOut.setValue(0);
    }
    
    List<String> terms = Lists.newArrayListWithCapacity(termIdMap.size());
    termIdMap.keysSortedByValue(terms);
    int i = 0;
    for (String t : terms) {
      float score = rank.get(i++);
      result.put(t, score);
      if (minXTermScoresOut != null && minXTermScoresOut.floatValue() > score) {
        minXTermScoresOut.setValue(score);
      }
      if (maxXTermScoresOut != null && maxXTermScoresOut.floatValue() < score) {
        maxXTermScoresOut.setValue(score);
      }
      if (totalXTermScoresOut != null) {
        totalXTermScoresOut.add(score);
      }
    }
    
    return result;
  }
  
  private final String timeFormatted;
  
  private String getTimeFormatted() {
    return timeFormatted;
  }
  
  // final QueryParser fisQparser;
  final IndexSearcher fisSearcher;
  final IndexReader fisIxReader;
  
  final int fisNumHits = NUM_HITS_INTERNAL_DEFAULT;
  final float fisBaseRankingParam = FIS_BASE_RANK_PARAM_DEFAULT;
  
  final Similarity fisSimilarity;
  
  // final QueryParser twtQparser;
  final IndexSearcher twtSearcher;
  final MultiReader twtIxReader;
  
  // final int twtNumHits = NUM_HITS_DEFAULT;
  
  final TwitterSimilarity twtSimilarity;
  
  float itemsetLenghtAvg = ITEMSET_LEN_AVG_DEFAULT;
  
  // As in Lambda of Jelink Mercer smoothing
  float itemsetCorpusModelWeight = ITEMSET_CORPUS_MODEL_WEIGHT_DEFAULT;
  float twitterCorpusModelWeight = TWITTER_CORPUS_MODEL_WEIGHT_DEFAULT;
  
  // As in Mue of Dirchilet smoothing
  float termWeightSmoother = TERM_WEIGHT_SMOOTHER_DEFAULT;
  
  float itemSetLenWght = ITEMSET_LEN_WEIGHT_DEFAULT;
  
  final long queryTime;
  
  // TODO command line
  // private float clusterSize = 33;
  
  private boolean fuzzyHashtag = false;
  
  private float minXTermScore;
  
  private float maxXTermScore;
  
  private boolean boostQuerySubsetByIdf = QUERY_SUBSET_BOOST_IDF_DEFAULT;
  
  // private boolean boostQuerySubsets = QUERY_SUBSET_BOOST_YESNO_DEFAULT;
  // public boolean isBoostQuerySubsets() {
  // return boostQuerySubsets;
  // }
  //
  // public void setBoostQuerySubsets(boolean boostQuerySubsets) {
  // this.boostQuerySubsets = boostQuerySubsets;
  // }
  
  public boolean isParseToTermQueries() {
    return parseToTermQueries;
  }
  
  public void setParseToTermQueries(boolean parseToTermQueries) {
    this.parseToTermQueries = parseToTermQueries;
  }
  
  private boolean parseToTermQueries = QUERY_PARSE_INTO_TERMS_DEFAULT;
  
  public float getTwitterCorpusModelWeight() {
    return twitterCorpusModelWeight;
  }
  
  public void setTwitterCorpusModelWeight(float twitterCorpusModelWeight) {
    this.twitterCorpusModelWeight = twitterCorpusModelWeight;
  }
  
  public float getTermWeightSmoother() {
    return termWeightSmoother;
  }
  
  public void setTermWeightSmoother(float termWeightSmoother) {
    this.termWeightSmoother = termWeightSmoother;
  }
  
  // public int getClusterSize() {
  // return clusterSize;
  // }
  //
  // public void setClusterSize(int clusterSize) {
  // this.clusterSize = clusterSize;
  // }
  
  public boolean isFuzzyHashtag() {
    return fuzzyHashtag;
  }
  
  public void setFuzzyHashtag(boolean fuzzyHashtag) {
    this.fuzzyHashtag = fuzzyHashtag;
  }
  
  public boolean isBoostQuerySubsetByIdf() {
    return boostQuerySubsetByIdf;
  }
  
  public void setBoostQuerySubsetByIdf(boolean boostQuerySubsetByIdf) {
    this.boostQuerySubsetByIdf = boostQuerySubsetByIdf;
  }
  
  private static boolean clusterClosedOnly = false;
  
  static boolean incremental = true;
  
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
    
    // fisQparser = new QueryParser(Version.LUCENE_36,
    // AssocField.STEMMED_EN.name,
    // tweetStemmingAnalyzer);
    // // ItemSetIndexBuilder.AssocField.ITEMSET.name,
    // // ANALYZER);
    // fisQparser.setDefaultOperator(Operator.AND);
    
    List<IndexReader> ixRds = Lists.newLinkedList();
    // long incrEndTime = openTweetIndexesBeforeQueryTime(twtIncIxLoc,
    // true,
    // false,
    // Long.MIN_VALUE,
    // ixRds);
    // if (twtChunkIxLocs != null) {
    // int i = 0;
    // long prevChunkEndTime = incrEndTime;
    // while (i < twtChunkIxLocs.length - 1) {
    // prevChunkEndTime = openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i++],
    // false,
    // false,
    // prevChunkEndTime,
    // ixRds);
    // }
    // openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i], false, true, prevChunkEndTime, ixRds);
    // }
    for (File ixLoc : IncrementalChuncksParallelHiers.getPathsBefore(twtIncIxLoc,
        twtChunkIxLocs,
        queryTime)) {
      ixRds.add(IndexReader.open(NIOFSDirectory.open(new File(ixLoc, "index"))));
    }
    twtIxReader = new MultiReader(ixRds.toArray(new IndexReader[0]));
    twtSearcher = new IndexSearcher(twtIxReader);
    twtSimilarity = new TwitterSimilarity();
    twtSearcher.setSimilarity(twtSimilarity);
    
    // twtQparser = new QueryParser(Version.LUCENE_36, TweetField.STEMMED_EN.name,
    // tweetStemmingAnalyzer);
    // // TweetField.TEXT.name, ANALYZER);
    // twtQparser.setDefaultOperator(Operator.AND);
    
    BooleanQuery.setMaxClauseCount(fisNumHits * fisNumHits);
  }
  
  public OpenIntFloatHashMap relatedItemsets(String queryStr, float minScore)
      throws IOException,
      org.apache.lucene.queryParser.ParseException, IllegalArgumentException, SecurityException,
      InstantiationException, IllegalAccessException, InvocationTargetException {
    if (queryStr == null || queryStr.isEmpty()) {
      throw new IllegalArgumentException("Query cannot be empty");
    }
    // if (minScore <= 0) {
    // throw new IllegalArgumentException("Minimum score must be positive");
    // }
    
    MutableLong qLen = new MutableLong(0);
    OpenObjectFloatHashMap<String> queryTermWeight = queryTermFreq(queryStr,
        qLen,
        (FISQueryExpander.SEARCH_NON_STEMMED ? FISQueryExpander.tweetNonStemmingAnalyzer
            : FISQueryExpander.tweetStemmingAnalyzer),
        (FISQueryExpander.SEARCH_NON_STEMMED ? AssocField.ITEMSET.name : AssocField.STEMMED_EN.name));
    // tweetStemmingAnalyzer,
    // AssocField.STEMMED_EN.name);
    // tweetNonStemmingAnalyzer,
    // AssocField.ITEMSET.name);
    
    Query query = parseQueryIntoTerms(
        queryTermWeight,
        qLen.floatValue(),
        // fisQparser,
        QueryParseMode.DISJUNCTIVE,
        false,
        (FISQueryExpander.SEARCH_NON_STEMMED ? AssocField.ITEMSET.name : AssocField.STEMMED_EN.name),
        // AssocField.ITEMSET.name,
        // AssocField.STEMMED_EN.name,
        fisIxReader);
    // QUERY_SUBSET_BOOST_YESNO_DEFAULT);
    
    OpenIntFloatHashMap resultSet = new OpenIntFloatHashMap();
    FISCollector bm25Coll = new FISCollector(this, queryStr, queryTermWeight, qLen.floatValue(),
        fisNumHits);
    fisSearcher.search(query, bm25Coll);
    
    TreeMap<ScoreIxObj<Integer>, String[]> rs = bm25Coll.getResultSet();
    
    Set<ScoreIxObj<String>> extraTerms = Sets.<ScoreIxObj<String>> newHashSet();
    Set<String> querySet = Sets.newCopyOnWriteArraySet(queryTermWeight.keys());
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
  
  // All the convertXXX functions are obsolete
  // public PriorityQueue<ScoreIxObj<String>> convertResultToWeightedTermsKLDivergence(
  // OpenIntFloatHashMap rs,
  // String query, int numItemsetsToConsider, boolean propagateISWeight,
  // MutableFloat minXTermScoreOut,
  // MutableFloat maxXTermScoreOut, MutableFloat totalTermScoreOut) throws IOException {
  //
  // if (minXTermScoreOut != null)
  // minXTermScoreOut.setValue(Float.MAX_VALUE);
  // if (maxXTermScoreOut != null)
  // maxXTermScoreOut.setValue(Float.MIN_VALUE);
  //
  // PriorityQueue<ScoreIxObj<String>> result = new PriorityQueue<ScoreIxObj<String>>();
  //
  // OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
  // OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query, null);
  // TransactionTree itemsets = convertResultToItemsetsInternal(rs,
  // queryFreq,
  // termIds,
  // numItemsetsToConsider, DEAFULT_MAGIC_ALLOWED);
  //
  // if (itemsets.isTreeEmpty()) {
  // return result;
  // }
  //
  // List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
  // termIds.keysSortedByValue(terms);
  //
  // OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>(terms.size());
  // float totalW = 0;
  //
  // Iterator<Pair<IntArrayList, Long>> itemsetIter = itemsets.iteratorClosed();
  // while (itemsetIter.hasNext()) {
  // Pair<IntArrayList, Long> patternPair = itemsetIter.next();
  //
  // float w = propagateISWeight ?
  // (patternPair.getSecond().floatValue()
  // / SCORE_PRECISION_MULTIPLIER) :
  // 1;
  // totalW += w;
  //
  // if (minXTermScoreOut != null && w < minXTermScoreOut.floatValue()) {
  // minXTermScoreOut.setValue(w);
  // }
  //
  // if (maxXTermScoreOut != null && w > maxXTermScoreOut.floatValue()) {
  // maxXTermScoreOut.setValue(w);
  // }
  //
  // IntArrayList pattern = patternPair.getFirst();
  // int pSize = pattern.size();
  // for (int i = 0; i < pSize; ++i) {
  // String ft = terms.get(pattern.getQuick(i));
  // if (queryFreq.containsKey(ft)) {
  // continue;
  // }
  // termFreq.put(ft, termFreq.get(ft) + w);
  // }
  // }
  //
  // for (String t : terms) {
  // if (queryFreq.containsKey(t)) {
  // continue;
  // }
  // // Collection metric FIXME : use stemmed
  // Term tTerm = new Term(TweetField.TEXT.name, t);
  // float docFreqC = twtIxReader.docFreq(tTerm);
  // if (docFreqC == 0) {
  // continue;
  // }
  // float pCollection = docFreqC / twtIxReader.numDocs();
  //
  // // KL Divergence considering all the itemsets as a document
  //
  // float pDoc = termFreq.get(t) / totalW;
  // float score = pDoc * (float) Math.log(pDoc / pCollection); // slow: MathUtils.log(2, pDoc /
  // // pCollection);
  // result.add(new ScoreIxObj<String>(t, score));
  // }
  //
  // if (totalTermScoreOut != null) {
  // totalTermScoreOut.setValue(totalW);
  // }
  //
  // return result;
  // }
  //
  // public PriorityQueue<ScoreIxObj<String>> convertResultToWeightedTermsConditionalProb(
  // OpenIntFloatHashMap rs,
  // String query, int numItemsetsToConsider, boolean propagateISWeight,
  // MutableFloat minXTermScoreOut,
  // MutableFloat maxXTermScoreOut, MutableFloat totalTermScoreOut) throws IOException {
  //
  // if (minXTermScoreOut != null)
  // minXTermScoreOut.setValue(Float.MAX_VALUE);
  //
  // if (maxXTermScoreOut != null)
  // maxXTermScoreOut.setValue(Float.MIN_VALUE);
  //
  // PriorityQueue<ScoreIxObj<String>> result = new PriorityQueue<ScoreIxObj<String>>();
  //
  // OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
  //
  // OpenObjectFloatHashMap<String> queryFreq = queryTermFreq(query, null);
  // TransactionTree itemsets = convertResultToItemsetsInternal(rs,
  // queryFreq,
  // termIds,
  // numItemsetsToConsider, DEAFULT_MAGIC_ALLOWED);
  //
  // if (itemsets.isTreeEmpty()) {
  // return result;
  // }
  //
  // List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
  // termIds.keysSortedByValue(terms);
  //
  // Set<String> querySet = Sets.newCopyOnWriteArraySet(queryFreq.keys());
  // Set<Set<String>> queryPowerSet = Sets.powerSet(querySet);
  //
  // int capacity = queryPowerSet.size() * (terms.size() - queryFreq.size());
  // OpenObjectFloatHashMap<Set<String>> subsetFreq = new OpenObjectFloatHashMap<Set<String>>(
  // Math.max(0, capacity));
  // OpenObjectFloatHashMap<String> termFreq = new OpenObjectFloatHashMap<String>(terms.size());
  // float totalW = 0;
  //
  // Iterator<Pair<IntArrayList, Long>> itemsetIter = itemsets.iteratorClosed();
  // while (itemsetIter.hasNext()) {
  // Pair<IntArrayList, Long> patternPair = itemsetIter.next();
  //
  // float w = propagateISWeight ?
  // (patternPair.getSecond().floatValue()
  // / SCORE_PRECISION_MULTIPLIER) :
  // 1;
  // totalW += w;
  //
  // if (minXTermScoreOut != null && w < minXTermScoreOut.floatValue()) {
  // minXTermScoreOut.setValue(w);
  // }
  //
  // if (maxXTermScoreOut != null && w > maxXTermScoreOut.floatValue()) {
  // maxXTermScoreOut.setValue(w);
  // }
  //
  // IntArrayList pattern = patternPair.getFirst();
  // int pSize = pattern.size();
  // Set<String> fisTerms = Sets.newHashSet();
  // for (int i = 0; i < pSize; ++i) {
  // fisTerms.add(terms.get(pattern.getQuick(i)));
  // }
  //
  // boolean weightNotAdded = true;
  // for (String ft : fisTerms) {
  // if (queryFreq.containsKey(ft)) {
  // continue;
  // }
  // termFreq.put(ft, termFreq.get(ft) + w);
  // for (Set<String> qSub : queryPowerSet) {
  // if (qSub.isEmpty()) {
  // continue;
  // }
  // if (!Sets.intersection(fisTerms, qSub).equals(qSub)) {
  // // This query s
  // continue;
  // } else if (weightNotAdded) {
  // subsetFreq.put(qSub, subsetFreq.get(qSub) + w);
  // }
  //
  // Set<String> key = Sets.union(qSub, ImmutableSet.<String> of(ft));
  // subsetFreq.put(key, subsetFreq.get(key) + w);
  // }
  // weightNotAdded = false;
  // }
  // }
  //
  // subsetFreq.put(ImmutableSet.<String> of(), totalW);
  // for (String t : terms) {
  // if (queryFreq.containsKey(t)) {
  // continue;
  // }
  //
  // float termCorpusQuality = 0;
  // if (twitterCorpusModelWeight > 0) {
  // // Collection metric FIXME use stemmed
  // Term tTerm = new Term(TweetField.TEXT.name, t);
  // float docFreqC = twtIxReader.docFreq(tTerm);
  // if (docFreqC == 0) {
  // continue;
  // }
  //
  // // odds of the term (not log odds)
  // termCorpusQuality = (termWeightSmoother + docFreqC) / twtIxReader.numDocs();
  // termCorpusQuality = termCorpusQuality / (1 - termCorpusQuality);
  // termCorpusQuality = (float) Math.log(termCorpusQuality);
  //
  // // IDF is has very large scale compared to probabilities
  // // float termCorpusQuality = (float) (Math.log(twtIxReader.numDocs() / (float) (docFreqC +
  // // 1)) + 1.0);
  // }
  //
  // // Query metric
  // Set<String> tAsSet = ImmutableSet.<String> of(t);
  // subsetFreq.put(tAsSet, termFreq.get(t));
  //
  // LinkedHashMap<Set<String>, Float> termQueryQuality = Maps
  // .<Set<String>, Float> newLinkedHashMap();
  //
  // for (Set<String> qSub : queryPowerSet) {
  //
  // float freqSub = subsetFreq.get(qSub);
  //
  // if (freqSub == 0) {
  // continue;
  // }
  //
  // Set<String> qSubExp = Sets.union(qSub, tAsSet);
  // float jointFreq = subsetFreq.get(qSubExp);
  //
  // // //Mutual information No normalization:
  // // if (jf != 0) {
  // // float jp = jf / totalW;
  // // numer += jp * (Math.log(jf / (ft1 * ft2)) + lnTotalW);
  // // denim += jp * Math.log(jp);
  // // }
  // float pExp = jointFreq / freqSub;
  //
  // termQueryQuality.put(qSubExp, pExp);
  // }
  //
  // float termQueryQualityAggr = 0;
  // for (String qTerm : querySet) {
  // // for (List<String> qtPair : Sets.cartesianProduct(tAsSet, querySet)) {
  // float qTermReps = queryFreq.get(qTerm);
  // List<String> qtPair = ImmutableList.<String> of(t, qTerm);
  // termQueryQualityAggr += qTermReps * aggregateTermQueryQuality(termQueryQuality,
  // Sets.newCopyOnWriteArraySet(qtPair),
  // termQueryQuality.get(tAsSet));
  // }
  // if (termQueryQualityAggr >= 1) {
  // // FIXME: This happens in case of repeated hashtag
  // termQueryQualityAggr = (float) (1 - 1E-6);
  // }
  //
  // termQueryQualityAggr /= (1 - termQueryQualityAggr);
  // termQueryQualityAggr = (float) Math.log(termQueryQualityAggr);
  //
  // float score = (float) (twitterCorpusModelWeight * termCorpusQuality +
  // (1 - twitterCorpusModelWeight) * termQueryQualityAggr);
  // result.add(new ScoreIxObj<String>(t, score));
  // }
  //
  // if (totalTermScoreOut != null) {
  // totalTermScoreOut.setValue(totalW);
  // }
  // return result;
  //
  // }
  //
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
        queryTermFreq(query, null, tweetNonStemmingAnalyzer, AssocField.ITEMSET.name),
        termIds,
        numResults, DEAFULT_MAGIC_ALLOWED);
    
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
      int numResults, boolean doMagic)
      throws IOException {
    OpenObjectFloatHashMap<Set<String>> itemsetRankFusion = new OpenObjectFloatHashMap<Set<String>>();
    OpenObjectFloatHashMap<Set<String>> itemsetWeight = new OpenObjectFloatHashMap<Set<String>>();
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    int rank = 1;
    for (int i = rs.size() - 1; i >= 0; --i) {
      if (numResults > 0 && rank > numResults) {
        break;
      }
      int hit = keyList.getQuick(i);
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          ItemSetIndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      Set<String> termSet = Sets.newCopyOnWriteArraySet(Arrays.asList(terms.getTerms()));
      
      if (doMagic) {
        float weight;
        if (itemsetWeight.containsKey(termSet)) {
          weight = itemsetWeight.get(termSet);
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
        
        itemsetWeight.put(termSet, weight);
      } else {
        Document doc = fisIxReader.document(hit);
        float patternFreq = Float.parseFloat(doc
            .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
            .stringValue());
        itemsetWeight.put(termSet, patternFreq);
      }
      itemsetRankFusion.put(termSet, rs.get(hit));
      ++rank;
    }
    
    TransactionTree result = new TransactionTree();
    int nextTerm = 0;
    LinkedList<Set<String>> keys = Lists.<Set<String>> newLinkedList();
    itemsetRankFusion.keysSortedByValue(keys);
    Iterator<Set<String>> isIter = keys.descendingIterator();
    int r = 0;
    while (isIter.hasNext() && (numResults <= 0 || r/* result.size() */< numResults)) {
      ++r;
      Set<String> is = isIter.next();
      long itemWeight = Math.round(MathUtils.round(itemsetRankFusion.get(is), 5)
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
  
  // All the convertXXX are obsolete
  // public Query convertResultToBooleanQuery(OpenIntFloatHashMap rs, String query, int numResults)
  // throws IOException, org.apache.lucene.queryParser.ParseException {
  // BooleanQuery result = new BooleanQuery();
  // PriorityQueue<ScoreIxObj<List<String>>> itemsets = convertResultToItemsets(rs,
  // query,
  // numResults);
  // while (!itemsets.isEmpty()) {
  // ScoreIxObj<List<String>> is = itemsets.poll();
  //
  // Query itemsetQuer = twtQparser.parse(is.obj.toString().replaceAll(COLLECTION_STRING_CLEANER,
  // ""));
  // if (QUERY_SUBSET_BOOST_YESNO_DEFAULT)
  // itemsetQuer.setBoost(is.score);
  // result.add(itemsetQuer, Occur.SHOULD);
  // }
  // return result;
  // }
  //
  // public List<Query> convertResultToQueries(OpenIntFloatHashMap rs,
  // String query, int numResults)
  // throws IOException, org.apache.lucene.queryParser.ParseException {
  // List<Query> result = Lists.<Query> newLinkedList();
  // PriorityQueue<ScoreIxObj<List<String>>> itemsets = convertResultToItemsets(rs,
  // query,
  // numResults);
  // while (!itemsets.isEmpty()) {
  // ScoreIxObj<List<String>> is = itemsets.poll();
  //
  // Query itemsetQuer = twtQparser.parse(is.obj.toString().replaceAll(COLLECTION_STRING_CLEANER,
  // ""));
  // if (QUERY_SUBSET_BOOST_YESNO_DEFAULT)
  // itemsetQuer.setBoost(is.score);
  // result.add(itemsetQuer);
  // }
  // return result;
  // }
  //
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
  
  private int addQualifiedResults(TreeMap<ScoreIxObj<Integer>, String[]> rs,
      OpenIntFloatHashMap resultSet,
      Set<String> queryTerms, Set<ScoreIxObj<String>> extraTerms, float minScore, float fusionK)
      throws IOException {
    int levelHits = 0;
    int rank = 0;
    minXTermScore = Float.MAX_VALUE;
    minXTermScore = Float.MIN_VALUE;
    for (ScoreIxObj<Integer> docIdScore : rs.keySet()) {
      if (docIdScore.score < minScore) {
        LOG.debug("Because of low score, pruning after result number {} out of {}",
            rank,
            rs.size());
        break;
      }
      
      // TermFreqVector termVector = fisIxReader.getTermFreqVector(scoreDoc.doc,
      // ItemSetIndexBuilder.AssocField.ITEMSET.name);
      // // TODO if(termVector.getTerms() not majority English or Proper names!!!!)
      // // continue;
      
      ++rank;
      
      int docId = docIdScore.obj;
      if (!resultSet.containsKey(docId)) { // scoreDoc.doc)) {
        ++levelHits;
      }
      
      float fusion = resultSet.get(docId); // scoreDoc.doc);
      fusion += 1.0f / (fusionK + rank);
      resultSet.put(docId, fusion);
      // scoreDoc.doc, fusion);
      
      if (fusion < minXTermScore) {
        minXTermScore = fusion;
      }
      
      if (fusion > maxXTermScore) {
        maxXTermScore = fusion;
      }
      
      Document doc = fisIxReader.document(docId);
      float patternFreq = Float.parseFloat(doc
          .getFieldable(ItemSetIndexBuilder.AssocField.SUPPORT.name)
          .stringValue());
      float termWeight = patternFreq;
      
      for (String term : rs.get(docIdScore)) {
        // termVector.getTerms()) {
        if (queryTerms.contains(term)) {
          continue;
        }
        // scoreDoc.score or fusion didn't change performance in a visible way
        extraTerms.add(new ScoreIxObj<String>(term, termWeight)); // TODO: is it better to ignore
                                                                  // this??
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
      IOException, IllegalArgumentException, SecurityException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    
    Set<ScoreIxObj<String>> extraTerms2 = Sets.<ScoreIxObj<String>> newHashSet();
    OpenObjectFloatHashMap<String> queryTermWeight2 = (OpenObjectFloatHashMap<String>) queryTermWeight
        .clone();
    
    // // This seems to suffer from the bad scoring of low overlap query
    // // +(q1 OR q2 OR q..) AND ONE_OF(xtra1 OR xtra2 OR ...)
    // // One query, and the first portion will limit the corpus
    // BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    // query.setMinimumNumberShouldMatch(1);
    //
    // // fisQparser.setDefaultOperator(Operator.OR);
    // // StringBuilder qtermsQueryStr = new StringBuilder();
    // // for (String qterm : queryTerms) {
    // // qtermsQueryStr.append(qterm).append(" ");
    // // }
    // // Query qtermsQuery = fisQparser.parse(qtermsQueryStr.toString());
    // Query qtermsQuery = parseQueryIntoTerms(queryTermWeight,
    // 0,
    // fisQparser,
    // QueryParseMode.DISJUNCTIVE,
    // false); // if boosting required, must pass actual length
    // query.add(new BooleanClause(qtermsQuery, Occur.MUST));
    //
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    // Query xtermQuery = new TermQuery(new Term(AssocField.ITEMSET.name, xterm.obj));
    // // Query xtermQuery = fisQparser.parse(xterm.obj);
    // // xtermQuery.setBoost(xterm.score);
    // queryTermWeight2.put(xterm.obj,xterm.score);
    // // Scoring the term not the query "+ xterm.toString()" causes an early topic drift
    //
    // query.add(xtermQuery, Occur.SHOULD);
    // }
    // // // Does this have any effect at all?
    // // query.setBoost(boost);
    // // fisQparser.setDefaultOperator(Operator.AND);
    // // TopDocs rs = fisSearcher.search(query, fisNumHits);
    // // int levelHits = addQualifiedResults(rs,
    // // resultSet,
    // // queryTerms,
    // // extraTerms2,
    // // levelMinScore,
    // // levelRankingParam);
    // // LOG.debug("Added {} results from the query {}", levelHits, query.toString());
    // ////////////////////////////////////////////////////////////////
    
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
    
    // // Only one query (a long one), of pairs of query and extra
    // // Seems to be getting high precision pairs, and performs best
    // BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    // for (String qterm : queryTermWeight.keys()) {
    // for (ScoreIxObj<String> xterm : extraTerms) {
    // assert !doneTerms.contains(xterm);
    //
    // float xtermWeight;
    // if (boostQuerySubsetByIdf) {
    // xtermWeight = twtSimilarity.idf(twtIxReader.docFreq(new Term(TweetField.TEXT.name,
    // xterm.obj)), twtIxReader.numDocs());
    // } else {
    // xtermWeight = xterm.score;
    // }
    //
    // Query xtermQuery = fisQparser.parse(qterm + "^" + queryTermWeight.get(qterm) + " "
    // + xterm.obj);
    // xtermQuery.setBoost(xtermWeight);
    // // Scoring the term not the query "+ xterm.toString()" causes an early topic drift
    //
    // query.add(xtermQuery, Occur.SHOULD);
    // }
    // }
    
    // Same as above technique but different coding.. should be faster
    // Only one query (a long one), of pairs of query and extra
    // Seems to be getting high precision pairs, and performs best
    BooleanQuery query = new BooleanQuery(); // true);
    for (ScoreIxObj<String> xterm : extraTerms) {
      assert !doneTerms.contains(xterm);
      // TODO: do we want to respect SEARCH_NON_STEMMED == false here?
      ScoreIxObj<Query> xtermQuery = createTermQuery(xterm,
          AssocField.ITEMSET.name,
          fisIxReader,
          minXTermScore,
          maxXTermScore, 1); // we don't want to weigh down those.. they are main:
                             // extraTerms.size());
      // xtermQuery.obj.setBoost(xtermQuery.score);
      queryTermWeight2.put(xterm.obj, (QUERY_SUBSET_BOOST_YESNO_DEFAULT ? xtermQuery.score : 1));
      
      for (String qterm : queryTermWeight.keys()) {
        
        BooleanQuery qxQuery = new BooleanQuery();
        // FIXME: Is this filtering so much, or just preventing drift?
        TermQuery qTermQuery = new TermQuery(new Term(AssocField.ITEMSET.name, qterm));
        
        qxQuery.add(qTermQuery, Occur.MUST);
        qxQuery.add((Query) xtermQuery.obj.clone(), Occur.MUST);
        
        // This is now done by putting scores as values of queryTerms map passed to BM25Collector
        // if (QUERY_SUBSET_BOOST_YESNO_DEFAULT) {
        // // qxQuery.setBoost(queryTermWeight.get(qterm) * xtermQuery.score);
        // qTermQuery.setBoost(queryTermWeight.get(qterm));
        // qxQuery.setBoost(xtermQuery.score);
        // }
        
        query.add(qxQuery, Occur.SHOULD);
      }
    }
    
    // // Does this have any effect at all?
    // query.setBoost(boost);
    // This is also not needed, the query is already so simple
    // query = (BooleanQuery) query.rewrite(fisIxReader);
    
    FISCollector bm25Coll = new FISCollector(this, query.toString(), queryTermWeight2, query
        .clauses().size(), fisNumHits);
    fisSearcher.search(query, bm25Coll);
    
    TreeMap<ScoreIxObj<Integer>, String[]> rs = bm25Coll.getResultSet();
    
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
  
  // public OpenObjectFloatHashMap<String> queryTermFreq(String query, MutableLong qLenOut)
  // throws IOException {
  // return queryTermFreq(query, qLenOut,
  // ANALYZER, TweetField.TEXT.name);
  // }
  
  public OpenObjectFloatHashMap<String> queryTermFreq(String query, MutableLong qLenOut,
      Analyzer pAnalyzer, String pFieldName)
      throws IOException {
    OpenObjectFloatHashMap<String> queryFreq = new OpenObjectFloatHashMap<String>();
    // String[] queryTokens = query.toString().split("\\W");
    TokenStream queryTokens = pAnalyzer.tokenStream(pFieldName,
        new StringReader(query.toString().trim()));
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
  
  // public Query parseQuery(String queryStr,
  // OpenObjectFloatHashMap<String> queryTermsOut,// = queryTermFreq(queryStr, qLen);
  // MutableLong qLenOut, // = new MutableLong(0);
  // QueryParser targetParser,
  // QueryParseMode mode, boolean boostQuerySubset)
  // throws IOException, org.apache.lucene.queryParser.ParseException {
  //
  // OpenObjectFloatHashMap<String> queryTermWeights = queryTermFreq(queryStr, qLenOut);
  //
  // Query result;
  // if (parseToTermQueries) {
  // result = parseQueryIntoTerms(queryTermWeights,
  // qLenOut.floatValue(),
  // // targetParser,
  // mode,
  // boostQuerySubset);
  // } else {
  // throw new UnsupportedOperationException();
  // // LOG.warn("Parsing to phrase Query is not thoroughly tested!");
  // // result = parseQueryIntoPhrase(queryStr, targetParser, mode);
  // }
  //
  // LOG.debug("Parsed \"{}\" into {}", queryStr, result);
  //
  // for (String key : queryTermWeights.keys())
  // queryTermsOut.put(key, queryTermWeights.get(key));
  //
  // return result;
  // }
  //
  // Query parseQueryIntoPhrase(String queryStr, QueryParser targetParser,
  // QueryParseMode mode) throws org.apache.lucene.queryParser.ParseException {
  // Operator op;
  // switch (mode) {
  // case CONJUNGTIVE:
  // op = Operator.AND;
  // break;
  // case DISJUNCTIVE:
  // op = Operator.OR;
  // break;
  // default:
  // throw new IllegalArgumentException();
  // }
  // Operator origOp = targetParser.getDefaultOperator();
  // targetParser.setDefaultOperator(op);
  // Query result;
  // try {
  // result = targetParser.parse(queryStr);
  // } finally {
  // targetParser.setDefaultOperator(origOp);
  // }
  // return result;
  // }
  
  public Query parseQueryIntoTerms(OpenObjectFloatHashMap<String> queryTermWeights,
      float qLen,
      // QueryParser targetParser,
      QueryParseMode mode, boolean boostQuerySubsets, String targetField, IndexReader targetReader)
      throws IOException, org.apache.lucene.queryParser.ParseException {
    float totalIDF = 0;
    if (boostQuerySubsets)
      if (boostQuerySubsetByIdf) {
        for (String qt : queryTermWeights.keys()) {
          float idf = twtSimilarity.idf(twtIxReader.docFreq(new Term(TweetField.TEXT.name, qt)),
              twtIxReader.numDocs());
          idf *= queryTermWeights.get(qt);
          totalIDF += idf;
          queryTermWeights.put(qt, idf);
        }
      }
    
    BooleanQuery query = new BooleanQuery(); // This adds trash: true);
    Set<String>[] querySets = null;
    switch (mode) {
    case CONJUNGTIVE:
      querySets = new Set[] { Sets.newCopyOnWriteArraySet(queryTermWeights.keys()) };
      break;
    case DISJUNCTIVE:
      querySets = new Set[queryTermWeights.keys().size()];
      int s = 0;
      for (String qt : queryTermWeights.keys()) {
        querySets[s++] = ImmutableSet.of(qt);
      }
      break;
    case POWERSET:
      querySets = Sets.powerSet(Sets.newCopyOnWriteArraySet(queryTermWeights.keys()))
          .toArray(new Set[0]);
      break;
    }
    
    // if (querySet.size() > 2) {
    for (Set<String> querySubSet : querySets) {
      // if (querySubSet.size() < 2) {
      if (querySubSet.isEmpty()) {
        continue;
      }
      
      BooleanQuery subQuery = new BooleanQuery();
      // targetParser.parse(querySubSet.toString()
      // .replaceAll(COLLECTION_STRING_CLEANER, ""));
      
      float querySubSetW = 0;
      
      for (String qTerm : querySubSet) {
        subQuery.add(createTermQuery(new ScoreIxObj<String>(qTerm, 1),
            targetField,
            targetReader,
            qLen,
            totalIDF, 1).obj,
            Occur.MUST);
        querySubSetW += queryTermWeights.get(qTerm);
      }
      
      if (boostQuerySubsets) {
        if (boostQuerySubsetByIdf) {
          querySubSetW /= totalIDF;
        } else {
          querySubSetW /= qLen;
        }
      } else {
        querySubSetW = 1;
      }
      
      subQuery.setBoost(querySubSetW);
      
      query.add(subQuery, Occur.SHOULD);
    }
    // } else {
    // Query subQuery = fisQparser.parse(querySet.toString().replaceAll(COLLECTION_STRING_CLEANER,
    // ""));
    // subQuery.setBoost(1);
    // query.add(subQuery, Occur.SHOULD);
    // }
    
    return query.rewrite(targetReader);
  }
  
  public Query expandAndFilterQuery(OpenObjectFloatHashMap<String> origQueryTerms,
      int origQueryLen,
      OpenObjectFloatHashMap<String>[] extraTerms,
      float[] minXTermScoreFloats, float[] maxXTermScoreFloats,
      int numTermsToAppend,
      OpenObjectFloatHashMap<String> xQueryTermsOut,
      MutableLong xQueryLenOut,
      ExpandMode mode) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    
    @SuppressWarnings("unchecked")
    LinkedList<String>[] xTermsKeys = new LinkedList[extraTerms.length];
    @SuppressWarnings("unchecked")
    Iterator<String>[] xTermsIter = new Iterator[extraTerms.length];
    @SuppressWarnings("unchecked")
    PriorityQueue<ScoreIxObj<String>>[] xTermHeaps = new PriorityQueue[extraTerms.length];
    for (int c = 0; c < extraTerms.length; ++c) {
      xTermsKeys[c] = Lists.newLinkedList();
      extraTerms[c].keysSortedByValue(xTermsKeys[c]);
      xTermsIter[c] = xTermsKeys[c].descendingIterator();
      xTermHeaps[c] = new PriorityQueue<ScoreIxObj<String>>();
    }
    
    Set<String> encounteredXTerms = Sets.newHashSet(origQueryTerms.keys());
    int t = 0;
    OpenIntHashSet emptyQueues = new OpenIntHashSet(extraTerms.length);
    while (t < numTermsToAppend) {
      for (int c = 0; c < extraTerms.length; ++c) {
        if (!xTermsIter[c].hasNext()) {
          if (!emptyQueues.contains(c)) {
            emptyQueues.add(c);
            if (EXPAND_TERM_COUNT_EVENLY_OVER_CLUSTERS)
              t += (numTermsToAppend - t) / extraTerms.length;
            
            if (emptyQueues.size() == extraTerms.length) {
              numTermsToAppend = -1;
              break;
            }
          }
          continue;
        }
        
        String xtermStr = xTermsIter[c].next();
        
        if (encounteredXTerms.contains(xtermStr)) {
          continue;
        } else {
          encounteredXTerms.add(xtermStr);
        }
        
        xTermHeaps[c].add(new ScoreIxObj<String>(xtermStr, extraTerms[c].get(xtermStr)));
        
        ++t;
      }
    }
    return expandAndFilterQuery(origQueryTerms,
        origQueryLen,
        xTermHeaps,
        minXTermScoreFloats,
        maxXTermScoreFloats,
        numTermsToAppend,
        xQueryTermsOut,
        xQueryLenOut,
        mode);
  }
  
  public Query expandAndFilterQuery(OpenObjectFloatHashMap<String> origQueryTerms,
      float origQueryLen,
      PriorityQueue<ScoreIxObj<String>>[] extraTerms,
      float[] minXTermScoreFloats, float[] maxXTermScoreFloats,
      float numTermsToAppend,
      OpenObjectFloatHashMap<String> xQueryTermsOut,
      MutableLong xQueryLenOut,
      ExpandMode mode) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    if (origQueryTerms.isEmpty() && mode.equals(ExpandMode.FILTERING)) {
      throw new IllegalArgumentException();
    }
    BooleanQuery result = new BooleanQuery();
    result.add(parseQueryIntoTerms(origQueryTerms,
        origQueryLen,
        // twtQparser,
        QueryParseMode.DISJUNCTIVE,
        QUERY_SUBSET_BOOST_YESNO_DEFAULT,
        (SEARCH_NON_STEMMED ? TweetField.TEXT.name : TweetField.STEMMED_EN.name),
        // TweetField.STEMMED_EN.name,
        twtIxReader),
        ExpandMode.FILTERING.equals(mode) ? Occur.MUST : Occur.SHOULD);
    
    if (xQueryLenOut != null)
      xQueryLenOut.setValue(origQueryLen);
    
    if (xQueryTermsOut != null) {
      for (String oTerm : origQueryTerms.keys()) {
        float value = origQueryTerms.get(oTerm);
        // Whatever I was doing earlier.. the term maps now store occurrence count for orig query
        xQueryTermsOut.put(oTerm, value);
        if (xQueryLenOut != null)
          xQueryLenOut.add(value);
      }
    }
    
    Set<String> encounteredXTerms = Sets.newHashSet(origQueryTerms.keys());
    int t = 0;
    OpenIntHashSet emptyQueues = new OpenIntHashSet(extraTerms.length);
    while (t < numTermsToAppend && extraTerms.length > 0) {
      for (int c = 0; c < extraTerms.length; ++c) {
        if (extraTerms[c].isEmpty()) {
          if (!emptyQueues.contains(c)) {
            emptyQueues.add(c);
            if (EXPAND_TERM_COUNT_EVENLY_OVER_CLUSTERS)
              t += (numTermsToAppend - t) / extraTerms.length;
            
            if (emptyQueues.size() == extraTerms.length) {
              numTermsToAppend = -1;
              break;
            }
          }
          continue;
        }
        
        ScoreIxObj<String> xterm = extraTerms[c].poll();
        
        if (!FISQueryExpander.SEARCH_NON_STEMMED) {
          xterm.obj = queryTermFreq(xterm.obj,
              null,
              FISQueryExpander.tweetStemmingAnalyzer, TweetField.STEMMED_EN.name).keys().get(0);
        }
        
        if (encounteredXTerms.contains(xterm.obj)) {
          continue;
        } else {
          encounteredXTerms.add(xterm.obj);
        }
        
        // Don't count hashtag and its exact word (probably from the twitter tokenizer)
        // without the # as distinct extra terms
        if (!encounteredXTerms.contains("#" + xterm.obj)) {
          ++t;
        }
        
        // Wholly shitt
        // if (xQueryTermsOut != null)
        // // Whatever I was doing earlier.. the term maps now store occurrence count
        // xQueryTermsOut.put(xterm.obj, 1);
        
        if (xQueryLenOut != null)
          xQueryLenOut.add(1);
        
        ScoreIxObj<Query> xtermQuery = createTermQuery(xterm,
            TweetField.TEXT.name,
            twtIxReader,
            minXTermScoreFloats[c],
            maxXTermScoreFloats[c], origQueryLen / numTermsToAppend);
        
        if (mode.equals(ExpandMode.FILTERING)) {
          for (String qterm : origQueryTerms.keys()) {
            BooleanQuery qxQuery = new BooleanQuery();
            Query qQuery = new TermQuery(new Term(TweetField.TEXT.name, qterm));
            
            qxQuery.add(qQuery, Occur.MUST);
            qxQuery.add(xtermQuery.obj, Occur.MUST);
            
            // // if (QUERY_SUBSET_BOOST_YESNO_DEFAULT) {
            // // qxQuery.setBoost(origQueryTerms.get(qterm) * xtermQuery.score);
            // qQuery.setBoost(origQueryTerms.get(qterm));
            // qxQuery.setBoost(xtermQuery.score);
            // // }
            float score = (QUERY_SUBSET_BOOST_YESNO_DEFAULT ? origQueryTerms.get(qterm) : 1);
            score *= xtermQuery.score; // / FIXMED: Always weigh down expantion terms
            if (Float.isInfinite(score) || Float.isNaN(score)) {
              score = 1E-6f;
            }
            xQueryTermsOut.put(xterm.obj, score);
            
            result.add(qxQuery, Occur.SHOULD);
          }
        } else if (mode.equals(ExpandMode.DIVERSITY)) {
          
          // // if (QUERY_SUBSET_BOOST_YESNO_DEFAULT) {
          // xtermQuery.obj.setBoost(xtermQuery.score);
          // // }
          
          if (Float.isInfinite(xtermQuery.score) || Float.isNaN(xtermQuery.score)) {
            xtermQuery.score = 1E-6f;
          }
          xQueryTermsOut.put(xterm.obj, xtermQuery.score);
          // (QUERY_SUBSET_BOOST_YESNO_DEFAULT ? xtermQuery.score : 1)); // FIXMED:Always // weigh
          // down
          // expansion
          // terms
          
          result.add(xtermQuery.obj, Occur.SHOULD);
          
        }
      }
    }
    
    LOG.debug("Expanded by {} terms. Query: {}", t, result);
    
    return filterQuery(result);
  }
  
  public Query expandAndFilterQuery(OpenObjectFloatHashMap<String> queryTerms,
      int queryLen, PriorityQueue<ScoreIxObj<String>>[] clustersTerms,
      MutableFloat[] minXTermScores,
      MutableFloat[] maxXTermScores, int numTermsToAppend,
      OpenObjectFloatHashMap<String> xQueryTermsOut,
      MutableLong xQueryLenOut,
      ExpandMode mode) throws IOException,
      org.apache.lucene.queryParser.ParseException {
    // convert mutables to floats and call
    float[] minXTermScoreFloats = new float[minXTermScores.length];
    float[] maxXTermScoreFloats = new float[maxXTermScores.length];
    for (int c = 0; c < minXTermScores.length; ++c) {
      minXTermScoreFloats[c] = minXTermScores[c].floatValue();
      maxXTermScoreFloats[c] = maxXTermScores[c].floatValue();
    }
    return expandAndFilterQuery(queryTerms,
        queryLen,
        clustersTerms,
        minXTermScoreFloats,
        maxXTermScoreFloats,
        numTermsToAppend, xQueryTermsOut, xQueryLenOut, mode);
  }
  
  ScoreIxObj<Query> createTermQuery(ScoreIxObj<String> xterm, String field,
      IndexReader targetReader, float minXTermScore, float maxXTermScore, float xtermWeightFactor)
      throws IOException {
    float xtermWeight = xtermWeightFactor;
    // This will be done by the IDF scoring
    // if (QUERY_SUBSET_BOOST_IDF) {
    // xtermWeight *= twtSimilarity.idf(twtIxReader.docFreq(new Term(TweetField.TEXT.name,
    // xterm.obj)), twtIxReader.numDocs());
    // } else {
    
    // Will always use this weight so it must be something well thought, the score is meaningless
    // xtermWeight *= (xterm.score - minXTermScore) / (maxXTermScore - minXTermScore);
    Term t = new Term(field, xterm.obj);
    Query result;
    if (fuzzyHashtag && xterm.obj.charAt(0) == '#') {
      // fuzzy matching for hashtags: #goodwoman #agoodwoman #goodwomen #goodman
      result = new FuzzyQuery(t, 0.85f, 1, 7);
      result = result.rewrite(targetReader);
    } else {
      result = new TermQuery(t);
    }
    return new ScoreIxObj<Query>(result, xtermWeight);
  }
  
  public Query filterQuery(Query q) throws IOException {
    // No retweets
    BooleanQuery result = new BooleanQuery();
    result.add(q, Occur.MUST);
    result.add(new TermQuery(new Term(TweetField.TEXT.name, RETWEET_TERM)), Occur.MUST_NOT);
    
    // Time filter
    NumericRangeFilter<Long> timeFilter = NumericRangeFilter
        .newLongRange(TweetField.TIMESTAMP.name,
            Long.MIN_VALUE,
            queryTime,
            true,
            true);
    
    return new FilteredQuery(result, timeFilter).rewrite(twtIxReader);
  }
  
  private static final boolean TERM_SCORE_PERCLUSTER = true;
  private static final float CLUSTER_MEMBERSHIP_THRESHOLD = 0.0f;
  
  // ////////////////////// TERM-TO-TERM ALL TERMS FEATURES //////////////////////////////
  
  Instances createTermTermMatrix(AbstractIntFloatMap rs, boolean weightInstances,
      OpenObjectIntHashMap<String> termIdMapOut,
      Set<Set<String>> itemsetsOut,
      TransactionTree patternTreeOut,
      boolean closedOnly,
      FastVector attrsOut) throws IOException {
    int nextId = 0;
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    for (int i = rs.size() - 1; i >= 0; --i) {
      int hit = keyList.getQuick(i);
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          ItemSetIndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      Set<String> termSet = Sets.newCopyOnWriteArraySet(Arrays.asList(terms.getTerms()));
      
      if (itemsetsOut.contains(termSet)) {
        continue;
      }
      
      itemsetsOut.add(termSet);
      
      IntArrayList pattern = new IntArrayList(termSet.size());
      
      for (String term : termSet) {
        if (!termIdMapOut.containsKey(term)) {
          termIdMapOut.put(term, nextId++);
          attrsOut.addElement(new Attribute(term));
        }
        pattern.add(termIdMapOut.get(term));
      }
      
      patternTreeOut.addPattern(pattern, 1);
    }
    if (patternTreeOut.isTreeEmpty()) {
      return null;
    }
    
    Instances insts = new Instances("term-term", attrsOut, attrsOut.size());
    
    Enumeration attrsEnum = attrsOut.elements();
    while (attrsEnum.hasMoreElements()) {
      Attribute termAttr = (Attribute) attrsEnum.nextElement();
      Term termT = new Term(TweetField.TEXT.name, termAttr.name());
      
      double weight = 1;
      if (weightInstances) {
        twtSimilarity.idf(twtIxReader.docFreq(termT),
            twtIxReader.numDocs());
      }
      Instance termInst = new Instance(weight, new double[attrsOut.size()]);
      
      termInst.setDataset(insts);
      insts.add(termInst);
    }
    return insts;
  }
  
  @SuppressWarnings("unchecked")
  public PriorityQueue<ScoreIxObj<String>>[] convertResultToWeightedTermsByClusteringTerms(
      OpenIntFloatHashMap rs, String query, boolean closedOnly,
      List<MutableFloat> minXTermScoresOut, List<MutableFloat> maxXTermScoresOut,
      List<MutableFloat> totalXTermScoresOut, boolean weightIDF) throws Exception {
    
    PriorityQueue<ScoreIxObj<String>>[] result = null;
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    Set<Set<String>> itemsets = Sets.newLinkedHashSet();
    TransactionTree patternTree = new TransactionTree();
    FastVector attrs = new FastVector();
    
    Instances insts = createTermTermMatrix(rs,
        weightIDF,
        termIdMap,
        itemsets,
        patternTree,
        closedOnly,
        attrs);
    
    if (patternTree.isTreeEmpty()) {
      return result;
    }
    Iterator<Pair<IntArrayList, Long>> patternsIter = patternTree.iterator(closedOnly);
    while (patternsIter.hasNext()) {
      IntArrayList pattern = patternsIter.next().getFirst();
      for (int i = 0; i < pattern.size(); ++i) {
        Instance inst = insts.instance(pattern.getQuick(i));
        for (int j = 0; j < pattern.size(); ++j) {
          int cooccurId = pattern.getQuick(j);
          if (i == cooccurId) {
            inst.setMissing(cooccurId);
          } else {
            inst.setValue(cooccurId, inst.value(cooccurId) + 1);
          }
        }
      }
    }
    if (insts == null || insts.numInstances() == 0) {
      return new PriorityQueue[0];
    }
    XMeans clusterer = new XMeans();
    ((XMeans) clusterer).setDistanceF(new CosineDistance());
    clusterer.buildClusterer(insts);
    
    LOG.info("Number of clusters: {}", clusterer.numberOfClusters());
    
    result = new PriorityQueue[clusterer.numberOfClusters()];
    if (TERM_SCORE_PERCLUSTER) {
      for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
        result[c] = new PriorityQueue<ScoreIxObj<String>>();
        if (minXTermScoresOut != null) {
          minXTermScoresOut.add(new MutableFloat(Float.MAX_VALUE));
        }
        if (maxXTermScoresOut != null) {
          maxXTermScoresOut.add(new MutableFloat(Float.MIN_VALUE));
        }
        if (totalXTermScoresOut != null) {
          totalXTermScoresOut.add(new MutableFloat(0));
        }
      }
      
      for (String term : termIdMap.keys()) {
        int termId = termIdMap.get(term);
        Instance inst = insts.instance(termId);
        double[] distrib = clusterer.distributionForInstance(inst);
        for (int c = 0; c < distrib.length; ++c) {
          if (distrib[c] <= CLUSTER_MEMBERSHIP_THRESHOLD) {
            continue;
          }
          
          // Probability is meaningless as score because it comes as either 0 or 1
          // float score = termDistrib[c];
          
          // Closeness to centroid
          Instance centroid = clusterer.getClusterCenters().instance(c);
          float score = 1 - (float) clusterer.getDistanceF().distance(centroid, inst);
          
          result[c].add(new ScoreIxObj<String>(term, score));
          
          if (minXTermScoresOut != null) {
            if (score < minXTermScoresOut.get(c).floatValue()) {
              minXTermScoresOut.get(c).setValue(score);
            }
          }
          
          if (maxXTermScoresOut != null) {
            if (score > maxXTermScoresOut.get(c).floatValue()) {
              maxXTermScoresOut.get(c).setValue(score);
            }
          }
          
          if (totalXTermScoresOut != null) {
            totalXTermScoresOut.get(c).add(score);
          }
        }
      }
      
    } else {
      throw new UnsupportedOperationException("TODO: change to avoid using the obsolete convertXX");
      // MutableFloat minScore = new MutableFloat(Float.MAX_VALUE);
      // MutableFloat maxScore = new MutableFloat(Float.MIN_VALUE);
      // MutableFloat totalScore = new MutableFloat(0);
      // PriorityQueue<ScoreIxObj<String>> termScores = convertResultToWeightedTermsKLDivergence(rs,
      // query,
      // -1,
      // false,
      // minScore,
      // maxScore,
      // totalScore);
      // // convertResultToWeightedTermsConditionalProb(rs,
      // // query,
      // // -1,
      // // false,
      // // minScore,
      // // maxScore,
      // // totalScore);
      //
      // for (int c = 0; c < result.length; ++c) {
      // result[c] = new PriorityQueue<ScoreIxObj<String>>();
      // if (minXTermScoresOut != null) {
      // minXTermScoresOut.add(minScore);
      // }
      //
      // if (maxXTermScoresOut != null) {
      // maxXTermScoresOut.add(maxScore);
      // }
      //
      // if (totalXTermScoresOut != null) {
      // totalXTermScoresOut.add(totalScore);
      // }
      //
      // PriorityQueue<ScoreIxObj<String>> termScoresClone = new
      // PriorityQueue<ScoreIxObj<String>>();
      // while (!termScores.isEmpty()) {
      // ScoreIxObj<String> scoredTerm = termScores.poll();
      // termScoresClone.add(scoredTerm);
      // int termId = termIdMap.get(scoredTerm.obj);
      // Instance inst = insts.instance(termId);
      // double[] distrib = clusterer.distributionForInstance(inst);
      // if (distrib[c] > CLUSTER_MEMBERSHIP_THRESHOLD) {
      // result[c].add(scoredTerm);
      // }
      // }
      // termScores = termScoresClone;
      //
      // }
    }
    return result;
  }
  
  public PriorityQueue<ScoreIxObj<String>> convertResultToWeightedTermsBySVDOfTerms(
      AbstractIntFloatMap rs, boolean closedOnly, boolean weightInsts) throws Exception {
    PriorityQueue<ScoreIxObj<String>> result = new PriorityQueue<ScoreIxObj<String>>();
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    Set<Set<String>> itemsets = Sets.newLinkedHashSet();
    TransactionTree patternTree = new TransactionTree();
    FastVector attrs = new FastVector();
    
    Instances insts = createTermTermMatrix(rs,
        weightInsts,
        termIdMap,
        itemsets,
        patternTree,
        closedOnly,
        attrs);
    if (insts != null) {
      // Removes attributes with only one distinct values (all of them), and so fails
      // try {
      // LatentSemanticAnalysis lsa = new LatentSemanticAnalysis();
      // lsa.buildEvaluator(insts);
      
      // create matrix of attribute values and compute singular value decomposition
      double[][] trainValues = new double[attrs.size()][insts.numInstances()];
      for (int i = 0; i < attrs.size(); i++) {
        trainValues[i] = insts.attributeToDoubleArray(i);
      }
      Matrix trainMatrix = new Matrix(trainValues);
      // svd requires rows >= columns, so transpose data if necessary
      if (attrs.size() < insts.numInstances()) {
        trainMatrix = trainMatrix.transpose();
      }
      SingularValueDecomposition trainSVD = trainMatrix.svd();
      Matrix s = trainSVD.getS(); // singular values
      
      Enumeration attrsEnum = attrs.elements();
      int i = 0;
      while (attrsEnum.hasMoreElements()) {
        Attribute attr = (Attribute) attrsEnum.nextElement();
        
        // float score = (float) lsa.evaluateAttribute(i);
        float score = (float) s.get(i, i);
        result.add(new ScoreIxObj<String>(attr.name(), score));
        i++;
      }
      // } catch (WekaException ingored) {
      // LOG.error(ingored.getMessage(), ingored);
      // }
    }
    return result;
  }
  
  // ////////////////////// PATTERN-TO-TERM ///////////////////////////////
  protected Instances createPatternTermMatrix(OpenIntFloatHashMap rs, boolean weightInsts,
      FastVector attrsOut, Map<IntArrayList, Instance> patternInstMapOut, boolean closedOnly,
      LinkedHashSet<Set<String>> itemsetsOut, boolean replaceMissing) throws Exception {
    
    OpenObjectIntHashMap<String> termIdMap = new OpenObjectIntHashMap<String>();
    
    TransactionTree patternTree = new TransactionTree();
    
    int nextId = 0;
    
    IntArrayList keyList = new IntArrayList(rs.size());
    rs.keysSortedByValue(keyList);
    for (int i = rs.size() - 1; i >= 0; --i) {
      int hit = keyList.getQuick(i);
      TermFreqVector terms = fisIxReader.getTermFreqVector(hit,
          ItemSetIndexBuilder.AssocField.ITEMSET.name);
      if (terms.size() < MIN_ITEMSET_SIZE) {
        continue;
      }
      Set<String> termSet = Sets.newCopyOnWriteArraySet(Arrays.asList(terms.getTerms()));
      
      if (itemsetsOut.contains(termSet)) {
        continue;
      }
      
      itemsetsOut.add(termSet);
      
      IntArrayList pattern = new IntArrayList(termSet.size());
      
      for (String term : termSet) {
        if (!termIdMap.containsKey(term)) {
          termIdMap.put(term, nextId++);
          attrsOut.addElement(new Attribute(term));
        }
        pattern.add(termIdMap.get(term));
      }
      
      patternTree.addPattern(pattern,
          Integer.parseInt(fisIxReader.document(hit).get(AssocField.SUPPORT.name)));
    }
    if (patternTree.isTreeEmpty()) {
      return null;
    }
    
    Instances insts = new Instances("pattern-term", attrsOut, itemsetsOut.size());
    
    patternTree = patternTree.getCompressedTree(closedOnly);
    Iterator<Pair<IntArrayList, Long>> patternsIter = patternTree.iterator();
    while (patternsIter.hasNext()) {
      Pair<IntArrayList, Long> pattern = patternsIter.next();
      Instance inst = new Instance(attrsOut.size());
      
      if (weightInsts) {
        inst.setWeight(pattern.getSecond());
      }
      
      IntArrayList patternItems = pattern.getFirst();
      for (int i = 0; i < patternItems.size(); ++i) {
        inst.setValue(patternItems.getQuick(i), 1);
      }
      if(replaceMissing){
        for (int i = 0; i < attrsOut.size(); ++i) {
          inst.setValue(i,0);
        }
      }
      
      insts.add(inst);
      patternInstMapOut.put(patternItems, inst);// do I need the actual???
                                                // insts.instance(insts.numInstances() - 1));
    }
    
    // This put the mean which is 1.. ya ahbal
//    ReplaceMissingValues replaceMissingFilter = new ReplaceMissingValues();
//    replaceMissingFilter.setInputFormat(insts);
//    insts = Filter.useFilter(insts, replaceMissingFilter);
    return insts;
  }
  
  @SuppressWarnings("unchecked")
  public PriorityQueue<ScoreIxObj<String>>[] convertResultToWeightedTermsByClusteringPatterns(
      OpenIntFloatHashMap rs, String query, boolean closedOnly,
      List<MutableFloat> minXTermScoresOut, List<MutableFloat> maxXTermScoresOut,
      List<MutableFloat> totalXTermScoresOut, boolean weightInsts) throws Exception {
    
    PriorityQueue<ScoreIxObj<String>>[] result = null;
    Map<IntArrayList, Instance> patternInstMap = Maps.newLinkedHashMap();
    LinkedHashSet<Set<String>> itemsets = Sets.newLinkedHashSet();
    FastVector attrs = new FastVector();
    Instances insts = createPatternTermMatrix(rs,
        weightInsts,
        attrs,
        patternInstMap,
        closedOnly,
        itemsets, true);
    if (insts == null || insts.numInstances() == 0) {
      return new PriorityQueue[0];
    }
    XMeans clusterer = new XMeans();
    clusterer.setDistanceF(new CosineDistance());
    clusterer.buildClusterer(insts);
    
    LOG.info("Number of clusters: {}", clusterer.numberOfClusters());
    
    OpenObjectFloatHashMap<String>[] termWeights;
    if (TERM_SCORE_PERCLUSTER) {
      termWeights = new OpenObjectFloatHashMap[clusterer.numberOfClusters()];
      
      for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
        termWeights[c] = new OpenObjectFloatHashMap<String>();
        
        if (minXTermScoresOut != null) {
          minXTermScoresOut.add(new MutableFloat(Float.MAX_VALUE));
        }
        if (maxXTermScoresOut != null) {
          maxXTermScoresOut.add(new MutableFloat(Float.MIN_VALUE));
        }
        if (totalXTermScoresOut != null) {
          totalXTermScoresOut.add(new MutableFloat(0));
        }
      }
      
      for (IntArrayList patternItems : patternInstMap.keySet()) {
        Instance inst = patternInstMap.get(patternItems);
        for (int i = 0; i < patternItems.size(); ++i) {
          int termId = patternItems.getQuick(i);
          String term = ((Attribute) attrs.elementAt(termId)).name();
          double[] distrib = clusterer.distributionForInstance(inst);
          for (int c = 0; c < distrib.length; ++c) {
            if (distrib[c] <= CLUSTER_MEMBERSHIP_THRESHOLD) {
              continue;
            }
            
            // Closeness to centroid
            Instance centroid = clusterer.getClusterCenters().instance(c);
            float score = 1 - (float) clusterer.getDistanceF().distance(centroid, inst);
            
            score += termWeights[c].get(term);
            termWeights[c].put(term, score);
            
            if (minXTermScoresOut != null) {
              if (score < minXTermScoresOut.get(c).floatValue()) {
                minXTermScoresOut.get(c).setValue(score);
              }
            }
            
            if (maxXTermScoresOut != null) {
              if (score > maxXTermScoresOut.get(c).floatValue()) {
                maxXTermScoresOut.get(c).setValue(score);
              }
            }
            
            if (totalXTermScoresOut != null) {
              totalXTermScoresOut.get(c).add(score);
            }
          }
        }
      }
    } else {
      throw new UnsupportedOperationException();
    }
    
    result = new PriorityQueue[clusterer.numberOfClusters()];
    OpenObjectFloatHashMap<String> queryTerms = queryTermFreq(query, null,
        (FISQueryExpander.SEARCH_NON_STEMMED ? FISQueryExpander.tweetNonStemmingAnalyzer
            : FISQueryExpander.tweetStemmingAnalyzer),
        (FISQueryExpander.SEARCH_NON_STEMMED ? TweetField.TEXT.name : TweetField.STEMMED_EN.name));
    for (int c = 0; c < clusterer.numberOfClusters(); ++c) {
      result[c] = new PriorityQueue<ScoreIxObj<String>>();
      for (String term : termWeights[c].keys()) {
        if (queryTerms.containsKey(term)) {
          continue;
        }
        result[c].add(new ScoreIxObj<String>(term, termWeights[c].get(term)));
      }
    }
    return result;
  }
  
  public PriorityQueue<ScoreIxObj<String>> convertResultToWeightedTermsBySVDOfPatternsMatrix(
      OpenIntFloatHashMap rs, boolean closedOnly, boolean weightInsts) throws Exception {
    PriorityQueue<ScoreIxObj<String>> result = new PriorityQueue<ScoreIxObj<String>>();
    
    Map<IntArrayList, Instance> patternInstMap = Maps.newLinkedHashMap();
    LinkedHashSet<Set<String>> itemsets = Sets.newLinkedHashSet();
    FastVector attrs = new FastVector();
    Instances insts = createPatternTermMatrix(rs,
        weightInsts,
        attrs,
        patternInstMap,
        closedOnly,
        itemsets, true);
    if (insts != null) {
      // Removes attributes with only one distinct values (all of them), and so fails
      // try {
      // LatentSemanticAnalysis lsa = new LatentSemanticAnalysis();
      // lsa.buildEvaluator(insts);
      
      // create matrix of attribute values and compute singular value decomposition
      double[][] trainValues = new double[attrs.size()][insts.numInstances()];
      for (int i = 0; i < attrs.size(); i++) {
        trainValues[i] = insts.attributeToDoubleArray(i);
      }
      Matrix trainMatrix = new Matrix(trainValues);
      // svd requires rows >= columns, so transpose data if necessary
      if (attrs.size() < insts.numInstances()) {
        trainMatrix = trainMatrix.transpose();
      }
      SingularValueDecomposition trainSVD = trainMatrix.svd();
      Matrix s = trainSVD.getS(); // singular values
      
      Enumeration attrsEnum = attrs.elements();
      int i = 0;
      while (attrsEnum.hasMoreElements()) {
        Attribute attr = (Attribute) attrsEnum.nextElement();
        
        // float score = (float) lsa.evaluateAttribute(i);
        float score = (float) s.get(i, i);
        result.add(new ScoreIxObj<String>(attr.name(), score));
        i++;
      }
      // } catch (WekaException ingored) {
      // LOG.error(ingored.getMessage(), ingored);
      // }
    }
    return result;
  }
}
