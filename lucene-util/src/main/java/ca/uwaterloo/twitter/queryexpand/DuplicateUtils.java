package ca.uwaterloo.twitter.queryexpand;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.math.list.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.ItemSetIndexBuilder;
import ca.uwaterloo.twitter.ItemSetIndexBuilder.AssocField;
import ca.uwaterloo.twitter.ItemSetSimilarity;
import ca.uwaterloo.twitter.TwitterAnalyzer;

import com.google.common.collect.Sets;

public class DuplicateUtils {
  
  private static final Logger LOG = LoggerFactory.getLogger(DuplicateUtils.class);
  private static final String COLLECTION_STRING_CLEANER = "[\\,\\[\\]]";
  
  private static final Analyzer ANALYZER = new TwitterAnalyzer();
  
  private  IndexReader fisIxReader;
  private  IndexSearcher fisSearcher;
  private  ItemSetSimilarity fisSimilarity;
  private  QueryParser fisQparser;
  
  
  
  /**
   * @param args
   * @throws CorruptIndexException 
   * @throws IOException
   * @throws ParseException
   */
  public DuplicateUtils(File pFisIncIxLocation) throws CorruptIndexException, IOException {
    Directory fisdir = new MMapDirectory(pFisIncIxLocation);
    fisIxReader = IndexReader.open(fisdir);
    fisSearcher = new IndexSearcher(fisIxReader);
    fisSimilarity = new ItemSetSimilarity();
    fisSearcher.setSimilarity(fisSimilarity);
    
    fisQparser = new QueryParser(Version.LUCENE_36,
        ItemSetIndexBuilder.AssocField.ITEMSET.name,
        ANALYZER);
    fisQparser.setDefaultOperator(Operator.AND);
  }
  
  public IntArrayList findDuplicates() throws IOException, ParseException{
    final IntArrayList toDelete = new IntArrayList();
    for (int d = 0; d < fisIxReader.maxDoc(); ++d) {
      final TermFreqVector termVector = fisIxReader.getTermFreqVector(d, AssocField.ITEMSET.name);
      final int doc1 = d;
      final int supp1 = Integer.parseInt(fisIxReader.document(doc1).get(AssocField.SUPPORT.name));
//      if (LOG.isTraceEnabled()) {
//        LOG.debug("Looking for duplicates for itemset {} with support {}",
//            termVector.getTerms(),
//            supp1);
//      }
      Query query = fisQparser.parse(Arrays.toString(termVector.getTerms())
          .replaceAll(COLLECTION_STRING_CLEANER, ""));
      fisSearcher.search(query, new Collector() {
        IndexReader reader;
        int docBase;
        
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          
        }
        
        @Override
        public void collect(int doc2) throws IOException {
          if (doc2 == doc1) {
            return;
          }
          TermFreqVector tv2 = fisIxReader.getTermFreqVector(doc2, AssocField.ITEMSET.name);
          Set<String> tSet1 = Sets.newCopyOnWriteArraySet(Arrays.asList(termVector.getTerms()));
          Set<String> tSet2 = Sets.newCopyOnWriteArraySet(Arrays.asList(tv2.getTerms()));
          Set<String> interSet = Sets.intersection(tSet1, tSet2);
          
          if (interSet.equals(tSet1)) {
            // This will be checked when doc2 is processed: || interSet.equals(tSet2)) {
            int supp2 = Integer.parseInt(fisIxReader.document(doc2).get(AssocField.SUPPORT.name));
            LOG.info("Itemset contained in another - itemset1: {}, itemset2: {}",
                Arrays.toString(termVector.getTerms()) + "/" + supp1,
                Arrays.toString(tv2.getTerms()) + "/" + supp2);
            toDelete.add(doc1);
          }
          //
          // if (tSet1.equals(tSet2)) {
          // LOG.info("Identical document number {} with term vector {}", doc2, tv2);
          // }
          //
          // Set<String> diff = Sets.difference(tSet1, tSet2);
          // if()
          
        }
        
        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
          this.reader = reader;
          this.docBase = docBase;
        }
        
        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }
        
      });
    }
    return toDelete;
  }
  
  public static void main(String[] args) throws CorruptIndexException, IOException, ParseException {
    new DuplicateUtils(new File("/u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-15min_1day/1295740800000/1295741700000/index")).findDuplicates();
  }
}
