package ca.uwaterloo.twitter;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.hadoop.util.CorpusReader;

public class ItemSetIndexBuilder {
  private static Logger LOG = LoggerFactory.getLogger(ItemSetIndexBuilder.class);
  
  public static enum AssocField {
    ID("id"),
    ITEMSET("itemset"),
    RANK("rank"),
    SUPPORT("support");
    
    public final String name;
    
    AssocField(String s) {
      name = s;
    }
  };
  
  private static final String INPUT_OPTION = "input";
  private static final String INDEX_OPTION = "index";
  
  private static final Analyzer ANALYZER = new TwitterAnalyzer(); // Version.LUCENE_36);
  
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
    
    buildIndex(new Path(seqPath), indexLocation);
  }
  
  public static void buildIndex(Path inPath, File indexDir) throws CorruptIndexException,
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
    
    IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir), config);
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
          
          int rank = 0;
          for (Pair<List<String>, Long> pattern : second.getPatterns()) {
            
            List<String> items = pattern.getFirst();
            if (items.size() < 2) {
              continue;
            }
            if (items.get(1).charAt(0) == '_') {
              // metadata
              // TODO: read probabilities of languages of patterns
              continue;
            }
            
            ++cnt;
            ++rank;
            
            Document doc = new Document();
            
            // Itemsets are sorted by term frequency, so they are not really sets
            // That is, I don't have to worry like I do below
            // Set<String> itemset = Sets.newCopyOnWriteArraySet(pattern.getFirst());
            
            // update the input of MD5
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.reset();
            md5.update(items.toString().getBytes());
            
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
            
            doc.add(new Field(AssocField.ID.name, hexString + "",
                Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            // doc.add(new Field(AssocField.ID.name,
            // items.toString().replaceAll("[\\S\\[\\(\\)\\]\\,]", "") + "",
            // Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            doc.add(new Field(AssocField.ITEMSET.name, items.toString(), Store.NO,
                Index.ANALYZED,
                TermVector.YES));
            
            doc.add(new Field(AssocField.RANK.name, rank + "",
                Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            doc.add(new Field(AssocField.SUPPORT.name, pattern.getSecond() + "",
                Store.YES, Index.NOT_ANALYZED_NO_NORMS));
            
            writer.addDocument(doc);
            if (cnt % 10000 == 0) {
              LOG.info(cnt + " itemsets indexed");
            }
          }
        }
      }
      
      LOG.info(String.format("Total of %s itemsets indexed", cnt));
      // Optimization Deprecated
      // LOG.info("Optimizing index...");
      // writer.optimize();
      
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
  
}
