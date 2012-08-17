package ca.uwaterloo.twitter;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.TwitterEnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import org.apache.mahout.freqtermsets.AggregatorReducer;
import org.apache.mahout.freqtermsets.TransactionTree;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.hadoop.util.CorpusReader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ItemSetIndexBuilder {
  public class DoneDeletingException extends RuntimeException {
    
    public DoneDeletingException() {
    }
    
    public DoneDeletingException(String message) {
      super(message);
    }
    
    public DoneDeletingException(Throwable cause) {
      super(cause);
    }
    
    public DoneDeletingException(String message, Throwable cause) {
      super(message, cause);
    }
    
  }
  
  private static Logger LOG = LoggerFactory
      .getLogger(ItemSetIndexBuilder.class);
  
  private static final double LOG2 = Math.log(2);
  
  public static enum AssocField {
    ID("id"), ITEMSET("itemset"), STEMMED_EN("stemmed"), RANK("rank"), SUPPORT(
        "support"), WINDOW_STARTTIME("window_start_time"), WINDOW_ENDTIME(
        "window_end_time");
    
    public final String name;
    
    AssocField(String s) {
      name = s;
    }
  };
  
  private static final String INPUT_OPTION = "input";
  private static final String INDEX_OPTION = "index";
  
  private static final Analyzer PLAIN_ANALYZER = new TwitterAnalyzer(); // Version.LUCENE_36);
  private static final Analyzer ENGLISH_ANALYZER = new TwitterEnglishAnalyzer();
  private static final Analyzer ANALYZER = new PerFieldAnalyzerWrapper(
      PLAIN_ANALYZER, ImmutableMap.<String, Analyzer> of(
          AssocField.STEMMED_EN.name, ENGLISH_ANALYZER));
  
  private static final boolean STATS = true;
  
  private static final boolean STORE_UNIGRAMS = false;
  private static final float MIN_UNI_SUPP_RATIO = 0.05f;
  private static final float MAX_UNI_SUPP_RATIO = 0.95f;
  private static final float ORIG_SUPPORT = 3;
  
  private static final boolean CLOSED_PATTERNS_ONLY = false;
  
  private QueryParser twtQparser;
  private IndexSearcher twtSearcher;
  private MultiReader twtIxReader;
  private Similarity twtSimilarity;
  
  public ItemSetIndexBuilder() {
    // List<IndexReader> ixRds = Lists.newLinkedList();
    // File twtIncIxLoc = new File(
    // "/u2/yaboulnaga/datasets/twitter-trec2011/index-stemmed_8hr-incremental");
    //
    // long now = System.currentTimeMillis();
    //
    // long incrEndTime = openTweetIndexesBeforeQueryTime(twtIncIxLoc,
    // true,
    // false,
    // Long.MIN_VALUE,
    // ixRds, now);
    // File[] twtChunkIxLocs = new File(
    // "/u2/yaboulnaga/datasets/twitter-trec2011/index-stemmed_chunks").listFiles();
    // if (twtChunkIxLocs != null) {
    // int i = 0;
    // long prevChunkEndTime = incrEndTime;
    // while (i < twtChunkIxLocs.length - 1) {
    // prevChunkEndTime =
    // openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i++],
    // false,
    // false,
    // prevChunkEndTime,
    // ixRds, now);
    // }
    // openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i], false, true,
    // prevChunkEndTime, ixRds, now);
    // }
    // twtIxReader = new MultiReader(ixRds.toArray(new IndexReader[0]));
    // twtSearcher = new IndexSearcher(twtIxReader);
    // twtSimilarity = new TwitterSimilarity();
    // twtSearcher.setSimilarity(twtSimilarity);
    //
    // twtQparser = new QueryParser(Version.LUCENE_36, TweetField.TEXT.name,
    // ANALYZER);
    // twtQparser.setDefaultOperator(Operator.AND);
    
  }
  
  /**
   * @param args
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws org.apache.lucene.queryParser.ParseException
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException,
      NoSuchAlgorithmException,
      org.apache.lucene.queryParser.ParseException {
    
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file")
        .create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: "
          + exp.getMessage());
      System.exit(-1);
    }
    
    if (!(cmdline.hasOption(INPUT_OPTION) && cmdline
        .hasOption(INDEX_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ItemSetIndexBuilder.class.getName(), options);
      System.exit(-1);
    }
    
    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    
    String seqPath = cmdline.getOptionValue(INPUT_OPTION);
    seqPath += File.separator + PFPGrowth.FREQUENT_PATTERNS; // "frequentpatterns";
    LOG.info("Indexing " + seqPath);
    
    ItemSetIndexBuilder builder = new ItemSetIndexBuilder();
    Map<String, SummaryStatistics> stats = builder.buildIndex(new Path(seqPath),
        indexLocation,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        null);
  }
  
  public Map<String, SummaryStatistics> buildIndex(Path inPath,
      File indexFile, long intervalStartTime, long intervalEndTime,
      Directory earlierIndex) throws CorruptIndexException,
      LockObtainFailedException, IOException, NoSuchAlgorithmException,
      org.apache.lucene.queryParser.ParseException {
    HashMap<String, SummaryStatistics> result = new HashMap<String, SummaryStatistics>();
    FileSystem fs = FileSystem.get(new Configuration());
    if (!fs.exists(inPath)) {
      LOG.error("Error: " + inPath + " does not exist!");
      throw new IOException("Error: " + inPath + " does not exist!");
    }
    
    CorpusReader<Writable, TopKStringPatterns> stream = new CorpusReader<Writable, TopKStringPatterns>(
        inPath, fs, "part.*");
    
    Analyzer analyzer = ANALYZER;
    Similarity similarity = new ItemSetSimilarity();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36,
        analyzer);
    config.setSimilarity(similarity);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); // Overwrite
    // existing.
    
    Directory indexDir = NIOFSDirectory.open(indexFile);
    IndexWriter writer = new IndexWriter(indexDir, config);
    if (earlierIndex != null) {
      // FIXME: this will delete the earlier index.. copy first.. and also
      // maybe it's good to delete docs before adding
      writer.addIndexes(earlierIndex);
    }
    try {
      
      Pair<Writable, TopKStringPatterns> p;
      int cnt = 0;
      
      TransactionTree closedPatterns = new TransactionTree();
      OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
      int nextTerm = 0;
      
      while ((p = stream.next()) != null) {
        
        Writable first = p.getFirst();
        TopKStringPatterns second = p.getSecond();
        
        if (second.getPatterns().size() == 0) {
          LOG.debug("Zero patterns for the feature: {}",
              first.toString());
          continue;
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Indexing: " + first.toString() + "\t"
              + second.toString());
        }
        
        for (Pair<List<String>, Long> patternPair : second
            .getPatterns()) {
          
          List<String> termSet = patternPair.getFirst();
          // if (termSet.size() < 2) {
          // continue;
          // }
          if (termSet.size() > 1 && termSet.get(1).charAt(0) == AggregatorReducer.METADATA_PREFIX) {
            // Will never happen now that it's space
            // metadata
            // TODONOT: read probabilities of languages of patterns
            continue;
          }
          
          ++cnt;
          if (cnt % 10000 == 0) {
            LOG.info(cnt + " patterns read");
          }
          
          IntArrayList patternInts = new IntArrayList(termSet.size());
          for (String term : termSet) {
            int termInt;
            if (termIds.containsKey(term)) {
              termInt = termIds.get(term);
            } else {
              termInt = nextTerm++;
              termIds.put(term, termInt);
            }
            patternInts.add(termInt);
          }
          
          closedPatterns.addPatternUnique(patternInts,
              patternPair.getSecond());
        }
      }
      
      LOG.info(String.format("Total of %s patterns read", cnt));
      
      if (closedPatterns.isTreeEmpty()) {
        return result;
        // continue;
      }
      
      closedPatterns = closedPatterns.getCompressedTree(CLOSED_PATTERNS_ONLY);
      
      List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
      termIds.keysSortedByValue(terms);
      
      int rank = 0;
      cnt = 0;
      
      float totalUnigramSupport = 0;
      float maxUnigramSupport = Float.MIN_VALUE;
      Iterator<Pair<IntArrayList, Long>> itemsetIter = closedPatterns
          .iterator(); // already closed only when compressing
      while (itemsetIter.hasNext()) {
        Pair<IntArrayList, Long> patternPair = itemsetIter.next();
        IntArrayList pattern = patternPair.getFirst();
        int pSize = pattern.size();
        
        if (pSize == 1) {
          Long supp = patternPair.getSecond();
          totalUnigramSupport += supp;
          if (supp > maxUnigramSupport) {
            maxUnigramSupport = supp;
          }
        }
        
        StringBuilder items = new StringBuilder();
        for (int i = 0; i < pSize; ++i) {
          String term = terms.get(pattern.getQuick(i));
          items.append(term).append(' ');
        }
        
        String itemsetStr = items.toString();
        Document doc = new Document();
        
        // Itemsets are sorted by term frequency, so they are not really
        // sets
        // That is, I don't have to worry like I do below
        // Set<String> itemset =
        // Sets.newCopyOnWriteArraySet(pattern.getFirst());
        
        // update the input of MD5
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.reset();
        md5.update(itemsetStr.getBytes());
        
        // get the output/ digest size and hash it
        byte[] digest = md5.digest();
        
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < digest.length; i++) {
          String byteStr = Integer.toHexString(0xFF & digest[i]);
          if (byteStr.length() < 2) {
            byteStr = "0" + byteStr;
          }
          hexString.append(byteStr);
        }
        
        if (earlierIndex != null) {
          // delete same document from earlier.. this one will have >=
          // support
          writer.deleteDocuments(new Term(AssocField.ID.name,
              hexString + ""));
        }
        
        doc.add(new Field(AssocField.ID.name, hexString + "",
            Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        
        // doc.add(new Field(AssocField.ID.name,
        // items.toString().replaceAll("[\\S\\[\\(\\)\\]\\,]", "") + "",
        // Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        
        doc.add(new Field(AssocField.ITEMSET.name, itemsetStr,
            Store.NO, Index.ANALYZED, TermVector.YES));
        doc.add(new Field(AssocField.STEMMED_EN.name, itemsetStr,
            Store.NO, Index.ANALYZED, TermVector.YES));// not enough
        // disk
        // space :(
        // YES));
        
        // No need to treat rankd and support as numeric fields.. will
        // never sort or filter
        
        // The rank is not reliable now that I have used a tree in the
        // middle.. I don't
        // know if the tree has any guarantees on iteration order.. I
        // should know! :(
        doc.add(new Field(AssocField.RANK.name, rank + "", Store.YES,
            Index.NOT_ANALYZED_NO_NORMS));
        ++rank;
        
        doc.add(new Field(AssocField.SUPPORT.name, patternPair
            .getSecond() + "", Store.YES,
            Index.NOT_ANALYZED_NO_NORMS));
        
        if (twtIxReader != null) {
          // FIXME????
          intervalEndTime = Long.MIN_VALUE;
          intervalStartTime = Long.MAX_VALUE;
          // Nothing called RangeQuery aslan.. beyshta3`aloony
          // Term begin = new Term(TweetField.TIMESTAMP.name, "" +
          // Long.MIN_VALUE);
          // Term end = new Term(TweetField.TIMESTAMP.name, "" +
          // Long.MAX_VALUE);
          // RangeQuery query = new RangeQuery(begin, end, true);
          // Slow as sala7ef
          // try {
          // Query query= twtQparser.parse(itemsetStr);
          // TopDocs topdoc = twtSearcher.search(query,1, new Sort(new
          // SortField(TweetField.TIMESTAMP.name, SortField.LONG,
          // false)));
          // if(topdoc.totalHits < 1){
          // LOG.warn("Got no tweets for the itemset {}", itemsetStr);
          // intervalStartTime = Long.MAX_VALUE;
          // } else {
          // intervalStartTime =
          // Long.parseLong(twtIxReader.document(topdoc.scoreDocs[0].doc).get(TweetField.TIMESTAMP.name));
          // }
          //
          // topdoc = twtSearcher.search(query,1, new Sort(new
          // SortField(TweetField.TIMESTAMP.name, SortField.LONG,
          // true)));
          // if(topdoc.totalHits < 1){
          // LOG.warn("Got no tweets for the itemset {}", itemsetStr);
          // intervalEndTime = Long.MIN_VALUE;
          // } else {
          // intervalEndTime =
          // Long.parseLong(twtIxReader.document(topdoc.scoreDocs[0].doc).get(TweetField.TIMESTAMP.name));
          // }
          //
          // } catch (org.apache.lucene.queryParser.ParseException e)
          // {
          // LOG.error(e.getMessage(), e);
          // intervalEndTime = Long.MIN_VALUE;
          // intervalStartTime = Long.MAX_VALUE;
          // }
        }
        doc.add(new NumericField(AssocField.WINDOW_STARTTIME.name,
            Store.YES, true).setLongValue(intervalStartTime));
        doc.add(new NumericField(AssocField.WINDOW_ENDTIME.name,
            Store.YES, true).setLongValue(intervalEndTime));
        
        writer.addDocument(doc);
        
        ++cnt;
        if (cnt % 10000 == 0) {
          LOG.info(cnt + " closed patterns indexed");
        }
      }
      
      LOG.info(String.format("Total of %s patterns indexed from compressed tree", cnt));
      
      // ////////////////////////////////////////////////////////
      // Delete duplicates
      writer.commit();
      // LOG.info("Optimizing index...");
      // writer.optimize();
      writer.close();
      
      int minUniSuppThreshold = Math
          .round(((ORIG_SUPPORT / maxUnigramSupport) + MIN_UNI_SUPP_RATIO) * maxUnigramSupport);
      int maxUniSuppThreshold = Math.round(MAX_UNI_SUPP_RATIO * maxUnigramSupport);
      
      final SummaryStatistics lengthStat;
      final SummaryStatistics supportStat;
      if (STATS) {
        lengthStat = new SummaryStatistics();
        supportStat = new SummaryStatistics();
        result.put("length", lengthStat);
        result.put("support", supportStat);
      }
      
      final IndexReader fisIxReader = IndexReader.open(indexDir, false);
      final IndexSearcher fisSearcher = new IndexSearcher(fisIxReader);
      final String COLLECTION_STRING_CLEANER = "[\\,\\[\\]]";
      final QueryParser fisQparser = new QueryParser(Version.LUCENE_36,
          ItemSetIndexBuilder.AssocField.ITEMSET.name, ANALYZER);
      fisQparser.setDefaultOperator(Operator.AND);
      
      // Deleting by docid is available only from reader
      // final IntArrayList toDelete = new IntArrayList();
      final OpenIntHashSet deleted = new OpenIntHashSet();
      for (int d = 0; d < fisIxReader.maxDoc(); ++d) {
        final TermFreqVector termVector = fisIxReader
            .getTermFreqVector(d, AssocField.ITEMSET.name);
        if (termVector == null) {
          LOG.error("Null term vector for document {} out of {}", d, fisIxReader.maxDoc());
          fisIxReader.deleteDocument(d);
          deleted.add(d);
          continue;
        }
        final Set<String> tSet1 = Sets.newCopyOnWriteArraySet(Arrays
            .asList(termVector.getTerms()));
        
        final int doc1 = d;
        final Document document = fisIxReader.document(doc1);
        final int supp1 = Integer.parseInt(document
            .get(AssocField.SUPPORT.name));
        
        if (STORE_UNIGRAMS && tSet1.size() == 1) {
          // double entropy = supp1 / totalUnigramSupport;
          // entropy = -entropy * Math.log(entropy) / LOG2;
          if (supp1 >= maxUniSuppThreshold || supp1 <= minUniSuppThreshold) {
            if (LOG.isDebugEnabled())
              LOG.debug(
                  "Unigram Itemset with support out of acceptable range {} - range: {}",
                  supp1, minUniSuppThreshold + " to " + maxUniSuppThreshold);
            
            fisIxReader.deleteDocument(doc1);
            // doc1Deleted.setValue(true);
            deleted.add(doc1);
            continue;
          }
        }
        
        //
        // final MutableBoolean doc1Deleted = new MutableBoolean(false);
        boolean doc1Deleted = false;
        // if (LOG.isTraceEnabled()) {
        // LOG.debug("Looking for duplicates for itemset {} with support {}",
        // termVector.getTerms(),
        // supp1);
        // }
        Query query = fisQparser.parse(tSet1.toString().replaceAll(
            COLLECTION_STRING_CLEANER, ""));
        Collector duplicateDeletionCollector = new Collector() {
          // IndexReader reader;
          int docBase;
          
          @Override
          public void setScorer(Scorer scorer) throws IOException {
            
          }
          
          @Override
          public void collect(int doc2) throws IOException {
            doc2 += docBase;
            if (doc2 == doc1 || /* doc1Deleted.booleanValue() || */deleted.contains(doc2)) {
              return;
            }
            TermFreqVector tv2 = fisIxReader.getTermFreqVector(
                doc2, AssocField.ITEMSET.name);
            Set<String> tSet2 = Sets.newCopyOnWriteArraySet(Arrays
                .asList(tv2.getTerms()));
            Set<String> interSet;
            if (CLOSED_PATTERNS_ONLY) {
              interSet = Sets.intersection(tSet1, tSet2);
            } else {
              // If we allow non-closed patterns, then we only need to delete
              // exact matches
              interSet = tSet2;
            }
            
            if (interSet.equals(tSet1)) {
              // This will be checked when doc2 is processed: ||
              // interSet.equals(tSet2)) {
              int supp2 = Integer.parseInt(fisIxReader.document(
                  doc2).get(AssocField.SUPPORT.name));
              if (LOG.isDebugEnabled())
                LOG.debug(
                    "Itemset contained in another - itemset1: {}, itemset2: {}",
                    tSet1
                        + "/" + supp1,
                    tSet2 + "/"
                        + supp2);
              
              fisIxReader.deleteDocument(doc1);
              deleted.add(doc1);
              
              // doc1Deleted.setValue(true);
              throw new DoneDeletingException();
            }
            
            //
            // if (tSet1.equals(tSet2)) {
            // LOG.info("Identical document number {} with term vector {}",
            // doc2, tv2);
            // }
            //
            // Set<String> diff = Sets.difference(tSet1, tSet2);
            // if()
            
          }
          
          @Override
          public void setNextReader(IndexReader reader, int docBase)
              throws IOException {
            // this.reader = reader;
            this.docBase = docBase;
          }
          
          @Override
          public boolean acceptsDocsOutOfOrder() {
            return true;
          }
        };
        try {
          fisSearcher.search(query, duplicateDeletionCollector);
        } catch (DoneDeletingException ignored) {
          doc1Deleted = true;
        }
        
        if (STATS && !doc1Deleted) { // !doc1Deleted.booleanValue()) {
          lengthStat.addValue(tSet1.size());
          supportStat.addValue(Integer.parseInt(document.get(AssocField.SUPPORT.name)));
        }
      }
      assert deleted.size() == fisIxReader.numDeletedDocs();
      fisIxReader.close();
      
      config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
      config.setSimilarity(similarity);
      config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
      writer = new IndexWriter(indexDir, config);
      // for (int docId : toDelete.keys()) {
      // Document document.get(AssocField.ID.name));
      // writer.deleteDocuments(new Term(AssocField.ID.name, id));
      // }
      // writer.commit();
      
      LOG.info("Deleted {} documents. Number of documents remaining: {}",
          deleted.size(),
          writer.numDocs());
      // Optimize is necessary for delete to have any effect.. even though
      // it is deprecated.. and this is not documented.. WTF!
      LOG.info("Optimizing index...");
      writer.optimize();
      LOG.info("After Optimization - NumDocs: {}, MaxDoc: {}", writer.numDocs(), writer.maxDoc());
      
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw ex;
    } catch (NoSuchAlgorithmException ex) {
      LOG.error(ex.getMessage(), ex);
      throw ex;
    } finally {
      writer.close();
      stream.close();
    }
    
    return result;
  }
  
}
