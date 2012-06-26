package ca.uwaterloo.twitter;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.corpus.data.CSVTweetRecordReader;

import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class TwitterIndexBuilder implements Callable<Void> {
  private static Logger LOG = LoggerFactory.getLogger(TwitterIndexBuilder.class);
  
  public static enum TweetField {
    ID("id"),
    SCREEN_NAME("screen_name"),
    TIMESTAMP("timestamp"),
    TEXT("text");
    
    public final String name;
    
    TweetField(String s) {
      name = s;
    }
  };
  
  private static final String INPUT_OPTION = "input";
  private static final String INDEX_OPTION = "index";
  private static final String THREADS_OPTION = "threads";
  private static final String DEFAULT_NUM_THREADS = "1";
  private static final String START_TIME_OPTION = "start";
  private static final String END_TIME_OPTION = "end";
  
  private static final Analyzer ANALYZER = new TwitterAnalyzer(); // Version.LUCENE_36);
  
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
    
//    LOG.info("Deleting {}", indexRoot);
//    FileUtils.deleteQuietly(indexRoot);
    if(indexRoot.exists()){
      LOG.error("Output folder already exists: {}", indexRoot);
      return;
    }
    
    int nThreads = Integer.parseInt(cmdline.getOptionValue(THREADS_OPTION, DEFAULT_NUM_THREADS));
    
    ExecutorService exec = Executors.newFixedThreadPool(nThreads);
    Future<Void> lastFuture = null;
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    
    String startTimeStr = cmdline.getOptionValue(START_TIME_OPTION);
    if (startTimeStr == null) {
      startTimeStr = fs.listStatus(seqRoot)[0].getPath().getName();
    }
    long startTime = Long.parseLong(startTimeStr);
    long endTime = Long.parseLong(cmdline.getOptionValue(END_TIME_OPTION, "" + Long.MAX_VALUE));
    
    for (FileStatus startFolder : fs.listStatus(seqRoot)) {
      long windowStart = Long.parseLong(startFolder.getPath().getName());
      if (windowStart < startTime) {
        continue;
      }
      for (FileStatus endFile : fs.listStatus(startFolder.getPath())) {
        long windowEnd = Long.parseLong(endFile.getPath().getName());
        if (windowEnd > endTime) {
          return;
        }
        
        File indexFile = new File(indexRoot, Long.toString(windowStart));
        indexFile = new File(indexFile, Long.toString(windowEnd));
        
        lastFuture = exec.submit(new TwitterIndexBuilder(endFile.getPath(), indexFile, conf));
      }
    }
    
    lastFuture.get();
    exec.shutdown();
    
    while (!exec.isTerminated()) {
      Thread.sleep(1000);
    }
    
  }
  
  private final Path inPath;
  private final File indexDir;
  private final Configuration conf;
  
  public TwitterIndexBuilder(Path inPath, File indexDir, Configuration conf) {
    super();
    this.inPath = inPath;
    this.indexDir = indexDir;
    this.conf = conf;
  }
  
  public Void call() throws Exception {
    
    FileSystem fs = FileSystem.get(new Configuration());
    
    LOG.info("Indexing {} to {}", inPath, indexDir);
    
    if (!fs.exists(inPath)) {
      LOG.error("Error: " + inPath + " does not exist!");
      throw new IOException("Error: " + inPath + " does not exist!");
    }
    
    CSVTweetRecordReader csvReader = new CSVTweetRecordReader();
    CombineFileSplit inputSplit = new CombineFileSplit(new Path[] { inPath }, new long[] { -1L });
    csvReader.initialize(inputSplit, conf);
    
    Analyzer analyzer = ANALYZER;
    Similarity similarity = new TwitterSimilarity();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
    config.setSimilarity(similarity);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE); // Overwrite existing.
    
    IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir), config);
    try {
      
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
        doc.add(new Field(TweetField.TEXT.name, tweet, Store.YES, Index.ANALYZED,
            TermVector.WITH_POSITIONS_OFFSETS));
        
        writer.addDocument(doc);
        if (++cnt % 10000 == 0) {
          LOG.info(cnt + " tweets indexed");
        }
      }
      
      LOG.info(String.format("Total of %s tweets indexed", cnt));
      // Optimization Deprecated
      // LOG.info("Optimizing index...");
      // writer.optimize();
      
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
