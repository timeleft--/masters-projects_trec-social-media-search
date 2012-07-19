package ca.uwaterloo.twitter;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.TwitterEnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.map.ser.impl.SimpleFilterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twitter.corpus.data.CSVTweetRecordReader;

import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class TwitterIndexBuilder implements Callable<Void> {
  private static Logger LOG = LoggerFactory.getLogger(TwitterIndexBuilder.class);
  
  public static enum TweetField {
    ID("id"),
    SCREEN_NAME("screen_name"),
    TIMESTAMP("timestamp"),
    TEXT("text"),
    STEMMED_EN("stemmed_en");
    
    public final String name;
    
    TweetField(String s) {
      name = s;
    }
  };
  
  private static final String INPUT_OPTION = "input";
  private static final String INDEX_OPTION = "index";
  private static final String THREADS_OPTION = "threads";
  private static final String DEFAULT_NUM_THREADS = "8";
  private static final String START_TIME_OPTION = "start";
  private static final String END_TIME_OPTION = "end";
  private static final String WINDOW_LEN_OPTION = "win";
  private static final String WINDOW_LEN_DEFAULT = "3600000";
  private static final boolean INCREMENTAL_DEFAULT = true;
  // /u2/yaboulnaga/datasets/twitter-trec2011/indexes/stemmed-stored_8hr-increments/1295740800000/1296633600000/index
  // start 1296604800000
  
  private static final Analyzer PLAIN_ANALYZER = new TwitterAnalyzer(); // Version.LUCENE_36);
  private static final Analyzer ENGLISH_ANALYZER = new TwitterEnglishAnalyzer();
  private static final Analyzer ANALYZER = new PerFieldAnalyzerWrapper(PLAIN_ANALYZER,
      ImmutableMap.<String, Analyzer> of(TweetField.STEMMED_EN.name, ENGLISH_ANALYZER));
  
  private static final boolean APPEND = false;
  private static final boolean TRUST_LUCENE_ADD_INDEX = true;
  private static final TermVector STORE_UNSTEMMED_TERMVECTOR = TermVector.NO;
  private static final TermVector STORE_STEMMED_TERMVECTOR = TermVector.YES;
  
  /**
   * @param args
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException,
      InterruptedException, ExecutionException {
    
    // TODO from commandline
    boolean incremental = INCREMENTAL_DEFAULT;
    
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input directory or file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("n").hasArg()
        .withDescription("(optional) number of threads to work on different time windows")
        .create(THREADS_OPTION));
    options.addOption(OptionBuilder.withArgName("time").hasArg()
        .withDescription("(optional) start time").create(START_TIME_OPTION));
    options.addOption(OptionBuilder.withArgName("time").hasArg()
        .withDescription("(optional) end time").create(END_TIME_OPTION));
    options.addOption(OptionBuilder.withArgName("length").hasArg()
        .withDescription("(optional) index window length").create(WINDOW_LEN_OPTION));
    
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
      formatter.printHelp(TwitterIndexBuilder.class.getName(), options);
      System.exit(-1);
    }
    
    File indexRoot = new File(cmdline.getOptionValue(INDEX_OPTION));
    
    Path seqRoot = new Path(cmdline.getOptionValue(INPUT_OPTION));
    
    // LOG.info("Deleting {}", indexRoot);
    // FileUtils.deleteQuietly(indexRoot);
    if (!APPEND && indexRoot.exists()) {
      LOG.error("Output folder already exists: {}", indexRoot);
      return;
    }
    
    int nThreads = Integer.parseInt(cmdline.getOptionValue(THREADS_OPTION, DEFAULT_NUM_THREADS));
    if (incremental && nThreads > 1) {
      throw new IllegalArgumentException("incremental and numThreads > 1");
    }
    
    ExecutorService exec = Executors.newFixedThreadPool(nThreads);
    Future<Void> lastFuture = null;
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf); // TODO: do I need? getLocal(conf);
    
    List<Path> startFoldersList = Lists.<Path> newLinkedList();
    // would have sorted the listStati right away, but the documentation for its compareTo
    // doesn't specify how the comparison is made.. a7eeh.
    for (FileStatus f : fs.listStatus(seqRoot)) {
      startFoldersList.add(f.getPath());
    }
    Path[] startFolders = startFoldersList.toArray(new Path[0]);
    Arrays.sort(startFolders);
    
    String startTimeStr = cmdline.getOptionValue(START_TIME_OPTION);
    if (startTimeStr == null) {
      startTimeStr = startFolders[0].getName();
    }
    
    long startTime = Long.parseLong(startTimeStr);
    long endTime = Long.parseLong(cmdline.getOptionValue(END_TIME_OPTION, "" + Long.MAX_VALUE));
    long winLen = Long.parseLong(cmdline.getOptionValue(WINDOW_LEN_OPTION, WINDOW_LEN_DEFAULT));
    
    List<Path> tweetFiles = Lists.newArrayListWithExpectedSize((int) winLen / 300000);
    
    long windowStart = -1;
    long folderStart = -1;
    int i = 0;
    while (i < startFolders.length) {
      folderStart = Long.parseLong(startFolders[i].getName());
      if (folderStart >= startTime) {
        break;
      }
      ++i;
    }
    
    windowStart = folderStart;
    
    Directory prevIndexDir = null;
    while (i < startFolders.length) {
      
      folderStart = Long.parseLong(startFolders[i].getName());
      List<Path> endFileList = Lists.newLinkedList();
      for (FileStatus f : fs.listStatus(startFolders[i])) {
        endFileList.add(f.getPath());
      }
      Path[] endFiles = endFileList.toArray(new Path[0]);
      Arrays.sort(endFiles);
      
      for (int j = 0; j < endFiles.length; ++j) {
        
        long fileEnd = Long.parseLong(endFiles[j].getName());
        
        if (fileEnd > windowStart + winLen) {
          File indexFile;
          if (incremental) {
            indexFile = new File(indexRoot, startTimeStr);
          } else {
            indexFile = new File(indexRoot, Long.toString(windowStart));
          }
          indexFile = new File(indexFile, Long.toString(windowStart + winLen));
          
          if (tweetFiles.size() > 0) {
            if (APPEND && indexFile.exists()) {
              // do nothing
            } else {
              lastFuture = exec.submit(new TwitterIndexBuilder(tweetFiles.toArray(new Path[0]),
                  indexFile, conf, prevIndexDir));
            }
            if (incremental) {
              // prevIndexDir = new MMapDirectory(indexFile);
              prevIndexDir = NIOFSDirectory.open(new File(indexFile, "index"));
            }
          } else {
            LOG.warn("No files for window: {} - {}", windowStart, windowStart + winLen);
          }
          
          tweetFiles.clear();
          // if (incremental) {
          // winLen += winLen;
          // } else {
          windowStart += winLen;
          if (fileEnd >= endTime) {
            return;
          }
        }
        
        if (fileEnd <= windowStart + winLen) {
          tweetFiles.add(endFiles[j]);
        }
        
      }
      
      ++i;
    }
    
    if (tweetFiles.size() > 0) {
      File indexFile;
      if (incremental) {
        indexFile = new File(indexRoot, startTimeStr);
      } else {
        indexFile = new File(indexRoot, Long.toString(windowStart));
      }
      indexFile = new File(indexFile, Long.toString(windowStart + winLen));
      if (APPEND && indexFile.exists()) {
        // do nothing
      } else {
        lastFuture = exec.submit(new TwitterIndexBuilder(tweetFiles.toArray(new Path[0]),
            indexFile, conf, prevIndexDir));
      }
    }
    
    lastFuture.get();
    exec.shutdown();
    
    while (!exec.isTerminated()) {
      Thread.sleep(1000);
    }
  }
  
  private final Path[] inPaths;
  private final File indexDir;
  private final Configuration conf;
  private final Directory prevIndexDir;
  
  public TwitterIndexBuilder(Path[] paths, File indexDir, Configuration conf, Directory prevIndexDir) {
    super();
    this.inPaths = paths;
    this.indexDir = indexDir;
    this.conf = conf;
    this.prevIndexDir = prevIndexDir;
  }
  
  public Void call() throws Exception {
    
    LOG.info("Indexing {} to {}", Arrays.toString(inPaths), indexDir);
    
    // FileSystem fs = FileSystem.get(new Configuration());
    // if (!fs.exists(inPath)) {
    // LOG.error("Error: " + inPath + " does not exist!");
    // throw new IOException("Error: " + inPath + " does not exist!");
    // }
    
    CSVTweetRecordReader csvReader = new CSVTweetRecordReader();
    CombineFileSplit inputSplit = new CombineFileSplit(inPaths, new long[] { -1L });
    csvReader.initialize(inputSplit, conf);
    
    Analyzer analyzer = ANALYZER;
    Similarity similarity = new TwitterSimilarity();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
    config.setSimilarity(similarity);
    
    FileUtils.deleteQuietly(indexDir);
    IndexWriter writer;
    
    if (!TRUST_LUCENE_ADD_INDEX && prevIndexDir != null) {
      
      config.setOpenMode(IndexWriterConfig.OpenMode.APPEND); // leave existing.
      
      FileUtils.copyDirectory(((FSDirectory) prevIndexDir).getDirectory(), indexDir);
      
    } else {
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); // Overwrite existing.
    }
    
    writer = new IndexWriter(FSDirectory.open(indexDir), config);
    
    try {
      
      if (TRUST_LUCENE_ADD_INDEX && prevIndexDir != null) {
        writer.addIndexes(prevIndexDir);
      }
      
      int cnt = 0;
      while (csvReader.nextKeyValue()) {
        PairOfLongs key = csvReader.getCurrentKey();
        long id = key.getLeftElement();
        long timestamp = key.getRightElement();
        
        PairOfStrings value = csvReader.getCurrentValue();
        String screenName = value.getLeftElement();
        String tweet = value.getRightElement();
        
        Document doc = new Document();
        
        doc.add(new Field(TweetField.ID.name, id + "",
            Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field(TweetField.SCREEN_NAME.name, screenName,
            Store.YES, Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new NumericField(TweetField.TIMESTAMP.name, Store.YES, true)
            .setLongValue(timestamp));
        doc.add(new Field(TweetField.TEXT.name, tweet, Store.YES,
            Index.ANALYZED, STORE_STEMMED_TERMVECTOR));
        doc.add(new Field(TweetField.STEMMED_EN.name, tweet, Store.NO,
            Index.ANALYZED, STORE_UNSTEMMED_TERMVECTOR));
        
        writer.addDocument(doc);
        if (++cnt % 10000 == 0) {
          LOG.info(cnt + " tweets indexed");
        }
      }
      
      LOG.info(String.format("Total of %s tweets indexed", cnt));
      
      LOG.info("Optimizing index...");
      writer.optimize();
      
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw ex;
    } finally {
      writer.close();
      csvReader.close();
    }
    return null;
  }
  
}
