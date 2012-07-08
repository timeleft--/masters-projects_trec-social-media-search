package ca.uwaterloo.twitter;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
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
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import org.apache.mahout.freqtermsets.TransactionTree;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.hadoop.util.CorpusReader;
import ca.uwaterloo.twitter.TwitterIndexBuilder.TweetField;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ItemSetIndexBuilder {
  private static Logger LOG = LoggerFactory.getLogger(ItemSetIndexBuilder.class);
  
  public static enum AssocField {
    ID("id"),
    ITEMSET("itemset"),
    STEMMED_EN("stemmed"),
    RANK("rank"),
    SUPPORT("support"),
    WINDOW_STARTTIME("window_start_time"),
    WINDOW_ENDTIME("window_end_time");
    
    public final String name;
    
    AssocField(String s) {
      name = s;
    }
  };
  
  private static final String INPUT_OPTION = "input";
  private static final String INDEX_OPTION = "index";
  
  private static final Analyzer PLAIN_ANALYZER = new TwitterAnalyzer(); // Version.LUCENE_36);
  private static final Analyzer ENGLISH_ANALYZER = new TwitterEnglishAnalyzer();
  private static final Analyzer ANALYZER = new PerFieldAnalyzerWrapper(PLAIN_ANALYZER,
      ImmutableMap.<String, Analyzer> of(AssocField.STEMMED_EN.name, ENGLISH_ANALYZER));
  private static final boolean STATS = true;
  
  private static QueryParser twtQparser;
  private static IndexSearcher twtSearcher;
  private static MultiReader twtIxReader;
  private static Similarity twtSimilarity;
  
  /**
   * @param args
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
    
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    
    if (!(cmdline.hasOption(INPUT_OPTION) && cmdline.hasOption(INDEX_OPTION))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ItemSetIndexBuilder.class.getName(), options);
      System.exit(-1);
    }
    
    File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
    
    String seqPath = cmdline.getOptionValue(INPUT_OPTION);
    seqPath += File.separator + PFPGrowth.FREQUENT_PATTERNS; // "frequentpatterns";
    LOG.info("Indexing " + seqPath);
    
    List<IndexReader> ixRds = Lists.newLinkedList();
    File twtIncIxLoc = new File(
        "/u2/yaboulnaga/datasets/twitter-trec2011/index-stemmed_8hr-incremental");
    
    long now = System.currentTimeMillis();
    
    long incrEndTime = openTweetIndexesBeforeQueryTime(twtIncIxLoc,
        true,
        false,
        Long.MIN_VALUE,
        ixRds, now);
    File[] twtChunkIxLocs = new File(
        "/u2/yaboulnaga/datasets/twitter-trec2011/index-stemmed_chunks").listFiles();
    if (twtChunkIxLocs != null) {
      int i = 0;
      long prevChunkEndTime = incrEndTime;
      while (i < twtChunkIxLocs.length - 1) {
        prevChunkEndTime = openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i++],
            false,
            false,
            prevChunkEndTime,
            ixRds, now);
      }
      openTweetIndexesBeforeQueryTime(twtChunkIxLocs[i], false, true, prevChunkEndTime, ixRds, now);
    }
    twtIxReader = new MultiReader(ixRds.toArray(new IndexReader[0]));
    twtSearcher = new IndexSearcher(twtIxReader);
    twtSimilarity = new TwitterSimilarity();
    twtSearcher.setSimilarity(twtSimilarity);
    
    twtQparser = new QueryParser(Version.LUCENE_36, TweetField.TEXT.name, ANALYZER);
    twtQparser.setDefaultOperator(Operator.AND);
    
    buildIndex(new Path(seqPath), indexLocation, Long.MIN_VALUE, Long.MAX_VALUE, null);
  }
  
  public static void buildIndex(Path inPath, File indexDir,
      long intervalStartTime, long intervalEndTime, Directory earlierIndex)
      throws CorruptIndexException,
      LockObtainFailedException, IOException, NoSuchAlgorithmException {
    
    FileSystem fs = FileSystem.get(new Configuration());
    if (!fs.exists(inPath)) {
      LOG.error("Error: " + inPath + " does not exist!");
      throw new IOException("Error: " + inPath + " does not exist!");
    }
    
    CorpusReader<Writable, TopKStringPatterns> stream = new CorpusReader<Writable, TopKStringPatterns>(
        inPath, fs, "part.*");
    
    Analyzer analyzer = ANALYZER;
    Similarity similarity = new ItemSetSimilarity();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
    config.setSimilarity(similarity);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); // Overwrite existing.
    
    SummaryStatistics lengthStat;
    SummaryStatistics supportStat;
    if (STATS) {
      lengthStat = new SummaryStatistics();
      supportStat = new SummaryStatistics();
    }
    
    IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir), config);
    if (earlierIndex != null) {
      writer.addIndexes(earlierIndex);
    }
    try {
      
      Pair<Writable, TopKStringPatterns> p;
      int cnt = 0;
      while ((p = stream.next()) != null) {
        
        Writable first = p.getFirst();
        TopKStringPatterns second = p.getSecond();
        
        if (second.getPatterns().size() == 0) {
          LOG.debug("Zero patterns for the feature: {}",
              first.toString());
        } else {
          if (LOG.isTraceEnabled())
            LOG.trace("Indexing: " + first.toString() + "\t" + second.toString());
          
          TransactionTree closedPatterns = new TransactionTree();
          OpenObjectIntHashMap<String> termIds = new OpenObjectIntHashMap<String>();
          int nextTerm = 0;
          
          for (Pair<List<String>, Long> patternPair : second.getPatterns()) {
            
            List<String> termSet = patternPair.getFirst();
            if (termSet.size() < 2) {
              continue;
            }
            if (termSet.get(1).charAt(0) == '_') {
              // metadata
              // TODO: read probabilities of languages of patterns
              continue;
            }
            
            if (STATS) {
              lengthStat.addValue(termSet.size());
              supportStat.addValue(patternPair.getSecond());
            }
            ++cnt;
            if (cnt % 10000 == 0) {
              LOG.info(cnt + " itemsets indexed");
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
            
            closedPatterns.addPattern(patternInts, patternPair.getSecond());
          }
          
          if (closedPatterns.isTreeEmpty()) {
            continue;
          }
          
          List<String> terms = Lists.newArrayListWithCapacity(termIds.size());
          termIds.keysSortedByValue(terms);
          
          int rank = 0;
          
          Iterator<Pair<IntArrayList, Long>> itemsetIter = closedPatterns.iterator(true);
          while (itemsetIter.hasNext()) {
            Pair<IntArrayList, Long> patternPair = itemsetIter.next();
            IntArrayList pattern = patternPair.getFirst();
            int pSize = pattern.size();
            StringBuilder items = new StringBuilder();
            for (int i = 0; i < pSize; ++i) {
              String term = terms.get(pattern.getQuick(i));
              items.append(term).append(' ');
            }
            
            String itemsetStr = items.toString();
            Document doc = new Document();
            
            // Itemsets are sorted by term frequency, so they are not really sets
            // That is, I don't have to worry like I do below
            // Set<String> itemset = Sets.newCopyOnWriteArraySet(pattern.getFirst());
            
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
              // delete same document from earlier.. this one will have >= support
              writer.deleteDocuments(new Term(AssocField.ID.name, hexString + ""));
            }
            
            doc.add(new Field(AssocField.ID.name, hexString + "",
                Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            // doc.add(new Field(AssocField.ID.name,
            // items.toString().replaceAll("[\\S\\[\\(\\)\\]\\,]", "") + "",
            // Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            doc.add(new Field(AssocField.ITEMSET.name, itemsetStr, Store.NO,
                Index.ANALYZED,
                TermVector.YES));
            doc.add(new Field(AssocField.STEMMED_EN.name, itemsetStr, Store.NO,
                Index.ANALYZED, TermVector.NO));// not enough disk space :( YES));
            
            // No need to treat rankd and support as numeric fields.. will never sort or filter
            
            // The rank is not reliable now that I have used a tree in the middle.. I don't
            // know if the tree has any guarantees on iteration order.. I should know! :(
            doc.add(new Field(AssocField.RANK.name, rank + "",
                Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            ++rank;
            
            doc.add(new Field(AssocField.SUPPORT.name, patternPair.getSecond() + "",
                Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            if (twtIxReader != null) {
              // FIXME????
              intervalEndTime = Long.MIN_VALUE;
              intervalStartTime = Long.MAX_VALUE;
              // Nothing called RangeQuery aslan.. beyshta3`aloony
              // Term begin = new Term(TweetField.TIMESTAMP.name, "" + Long.MIN_VALUE);
              // Term end = new Term(TweetField.TIMESTAMP.name, "" + Long.MAX_VALUE);
              // RangeQuery query = new RangeQuery(begin, end, true);
              // Slow as sala7ef
              // try {
              // Query query= twtQparser.parse(itemsetStr);
              // TopDocs topdoc = twtSearcher.search(query,1, new Sort(new
              // SortField(TweetField.TIMESTAMP.name, SortField.LONG, false)));
              // if(topdoc.totalHits < 1){
              // LOG.warn("Got no tweets for the itemset {}", itemsetStr);
              // intervalStartTime = Long.MAX_VALUE;
              // } else {
              // intervalStartTime =
              // Long.parseLong(twtIxReader.document(topdoc.scoreDocs[0].doc).get(TweetField.TIMESTAMP.name));
              // }
              //
              // topdoc = twtSearcher.search(query,1, new Sort(new
              // SortField(TweetField.TIMESTAMP.name, SortField.LONG, true)));
              // if(topdoc.totalHits < 1){
              // LOG.warn("Got no tweets for the itemset {}", itemsetStr);
              // intervalEndTime = Long.MIN_VALUE;
              // } else {
              // intervalEndTime =
              // Long.parseLong(twtIxReader.document(topdoc.scoreDocs[0].doc).get(TweetField.TIMESTAMP.name));
              // }
              //
              // } catch (org.apache.lucene.queryParser.ParseException e) {
              // LOG.error(e.getMessage(), e);
              // intervalEndTime = Long.MIN_VALUE;
              // intervalStartTime = Long.MAX_VALUE;
              // }
            }
            doc.add(new NumericField(AssocField.WINDOW_STARTTIME.name, Store.YES, true)
                .setLongValue(intervalStartTime));
            doc.add(new NumericField(AssocField.WINDOW_ENDTIME.name, Store.YES, true)
                .setLongValue(intervalEndTime));
            
            writer.addDocument(doc);
            
          }
        }
      }
      
      LOG.info(String.format("Total of %s itemsets indexed", cnt));
      
      if (STATS) {
        FileUtils
            .writeStringToFile(new File(
                "/u2/yaboulnaga/datasets/twitter-trec2011/assoc-mr_0607-2100/length-stats_index-closed.txt"),
                lengthStat.toString());
        FileUtils
        .writeStringToFile(new File(
            "/u2/yaboulnaga/datasets/twitter-trec2011/assoc-mr_0607-2100/support-stats_index-closed.txt"),
            supportStat.toString());
      }
      LOG.info("Optimizing index...");
      writer.optimize();
      
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
  }
  
  private static long openTweetIndexesBeforeQueryTime(File twtIndexLocation, boolean pIncremental,
      boolean exceedTime, long windowStart, List<IndexReader> ixReadersOut, long queryTime)
      throws IOException {
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
    // if (minIx == maxIx) {
    // startFolders = new File[] { startFolders[minIx] };
    // } else {
    // startFolders = Arrays.copyOfRange(startFolders, minIx, maxIx);
    // }
    if (minIx == -1) {
      // This chunk ended where it should have started
      return windowStart;
    }
    startFolders = Arrays.copyOfRange(startFolders, minIx, maxIx + 1);
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
}
