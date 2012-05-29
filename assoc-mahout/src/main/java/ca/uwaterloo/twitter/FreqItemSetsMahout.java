package ca.uwaterloo.twitter;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPGrowth;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.utils.vectors.TermEntry;
import org.apache.mahout.utils.vectors.TermInfo;
import org.apache.mahout.utils.vectors.lucene.CachedTermInfo;
import org.apache.mahout.utils.vectors.lucene.LuceneIterable;
import org.apache.mahout.utils.vectors.lucene.LuceneIterator;
import org.apache.mahout.utils.vectors.lucene.TFDFMapper;
import org.apache.mahout.utils.vectors.lucene.VectorMapper;
import org.apache.mahout.vectorizer.Weight;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class FreqItemSetsMahout {
	private static final Logger LOG = Logger.getLogger(FreqItemSetsMahout.class);
	
	
	
public static void main(String[] args) {
	String pIxPath = "D:\\datasets\\twitter-trec2011\\index";
	String pArffPath = "D:\\datasets\\twitter-trec2011\\arff";
	String pFieldName = "text";
	int pMinDf = 100;
	int pMaxDFPercent = 95;

	File ixFile = new File(pIxPath);
	Directory ixDir = new MMapDirectory(ixFile);
	final IndexReader ixRd = IndexReader.open(ixDir);
	try {
		LOG.info("Opened index: " + pIxPath);
		
		
		TermInfo termInfo = new CachedTermInfo(ixRd, pFieldName, pMinDf, pMaxDFPercent);
		List<Pair<String,Long>> freqList = new ArrayList<Pair<String,Long>>(termInfo.totalTerms(pFieldName));
		Iterator<TermEntry> termIter = termInfo.getAllEntries();
		while(termIter.hasNext()){
			TermEntry term = termIter.next();
			//TODO If we can change below to Int, change here too
			freqList.add(new Pair<String, Long>(Integer.toString(term.getTermIdx()), new Long(term.getDocFreq())));
		}
		
		Weight boolWeight = new Weight() {
			public double calculate(int tf, int df, int length, int numDocs) {
				return 1;
			}
		};
	    VectorMapper mapper = new TFDFMapper(ixRd, boolWeight, termInfo);
	    
	    Iterator<Pair<List<String>,Long>> ixIter = Iterators.transform(new LuceneIterator(ixRd, "id", pFieldName, mapper, LuceneIterable.NO_NORMALIZING),
	    		new Function<Vector, Pair<List<String>,Long>>(){

					public Pair<List<String>, Long> apply(Vector input) {
						checkNotNull(input); // for GWT
					    ArrayList<String> terms = new ArrayList<String>(input.getNumNondefaultElements());
					    Iterator<Element> iter = input.iterateNonZero();
					    while (iter.hasNext()) {
					    	Element elt = iter.next();
					    	// TODO: See if we really need to keep it a streing or we can use an int
					    	terms.add(Integer.toString(elt.index()));
					    }
					    
						return new Pair<List<String>, Long>(terms, weight);
					}
	    });
	    
	    
	    FPGrowth<String> fp = new FPGrowth<String>();
	    fp.generateTopKFrequentPatterns(ixIter, frequencyList, minSupport, k, returnableFeatures, output, updater)
	} finally {
		ixRd.close();
	}
	
	FPGrowth<String> fp = new FPGrowth<String>();
	 Set<String> features = new HashSet<String>();
	 fp.generateTopKStringFrequentPatterns(
	     new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
	        fp.generateFList(
	          new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern), minSupport),
	         minSupport,
	        maxHeapSize,
	        features,
	        new StringOutputConvertor(new SequenceFileOutputCollector<Text, TopKStringPatterns>(writer))
	  );
}
}
