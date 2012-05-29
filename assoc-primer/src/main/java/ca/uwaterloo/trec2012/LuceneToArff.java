package ca.uwaterloo.trec2012;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

import uwaterloo.util.NotifyStream;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.core.converters.ArffSaver;

public class LuceneToArff {
	private static Logger L = Logger.getLogger("LuceneToArff");
	
	private static final boolean PRESERVE_TERM_FREQUENCY = false;
	private static final String TXT_FIELD = "text";
	public static final String FALSE = "F";
	public static final String TRUE = "T";
	private static final int MIN_DOCUMENT_FREQ = 100;
	private static final int SLICE_SIZE = 1000;
	private static final int NUM_WRITE_THREADS = 2;
	
	static class SliceDocsTransformer implements Callable<Instances> {

		final IndexReader ixRd;
		final int sStart;
		double numDocs;
		final FastVector tVector;
		final String arffPath;

		public SliceDocsTransformer(int s, int numDocs, FastVector tVector,
				IndexReader ixRd, String arffPath) {
			this.sStart = s;
			this.numDocs = numDocs;
			this.tVector = tVector;
			this.ixRd = ixRd;
			this.arffPath = arffPath;
		}

		public Instances call() throws Exception {
			int sliceMax = sStart + (SLICE_SIZE - 1);

			Instances result = new Instances("trec2011-twitter_" + sStart + "-"
					+ sliceMax, tVector, SLICE_SIZE);

			for (int i = 0; i < SLICE_SIZE && (sStart + i < numDocs); ++i) {
				SparseInstance inst = new SparseInstance(tVector.size());
				result.add(inst);
			}

			TermEnum tEnum = ixRd.terms();
			int tNum = 0;
			while (tEnum.next()) {
				Term t = tEnum.term();

				int df = ixRd.docFreq(t);
				if (df < MIN_DOCUMENT_FREQ || !(t.field().equals(TXT_FIELD))) {
					continue;
				}

				double idf = Math.log(numDocs / df);

				TermDocs tDocs = ixRd.termDocs(t);

				tDocs.skipTo(sStart);
				int stDocs = 0;
				do {
					int d = tDocs.doc();
					if (d > sliceMax)
						break;

					++stDocs;
					Instance inst = result.instance(d % SLICE_SIZE);
					if (PRESERVE_TERM_FREQUENCY) {
						inst.setValue(tNum, idf);
					} else {
						inst.setValue(tNum, TRUE);
					}

				} while (tDocs.next());

				if (L.isTraceEnabled())
					L.trace(String
							.format("Term %s found in %d docs within slice starting %d, idf = %f",
									t.text(), stDocs, sStart, idf));

				++tNum;
				if (tNum % 1000 == 0) {
					L.info(String.format(
							"Added %d tokens to arff of slice starting %d",
							tNum, sStart));
				}
			}

			File arffFile = FileUtils.getFile(arffPath, "slice_" + sStart + "-"
					+ sliceMax + ".arff");
			L.info("Writing out: " + arffFile);
			
			
			
			ArffSaver arffS = new ArffSaver();
			arffS.setDestination(FileUtils.openOutputStream(arffFile));
			arffS.setInstances(result);
			arffS.setCompressOutput(true);
			arffS.writeBatch();

			return result;
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig, "LuceneToArff");
		try {
			System.setErr(new PrintStream(notifyStream));

			String ixPath = "D:\\datasets\\twitter-trec2011\\index";
			String arffPath = "D:\\datasets\\twitter-trec2011\\arff";

			File ixFile = new File(ixPath);
			Directory ixDir = new MMapDirectory(ixFile);
			final IndexReader ixRd = IndexReader.open(ixDir);
			try {
				L.info("Opened index: " + ixPath);
				FastVector tVector = new FastVector(); // Unsupported:
														// (int)ixRd.getUniqueTermCount());
				FastVector boolVector = new FastVector(2);
				// boolVector.addElement(FALSE);//Boolean.FALSE.toString());
				boolVector.addElement(TRUE);// Boolean.TRUE.toString());
				TermEnum tEnum = ixRd.terms();
				int i = 0;
				while (tEnum.next()) {
					Term t = tEnum.term();

					int df = ixRd.docFreq(t);
					if (df < MIN_DOCUMENT_FREQ
							|| !(t.field().equals(TXT_FIELD))) {
						continue;
					}

					if (PRESERVE_TERM_FREQUENCY) {
						tVector.addElement(new Attribute(t.text()));
					} else {
						tVector.addElement(new Attribute(t.text(), boolVector));
					}
					++i;
					if (i % 1000 == 0) {
						L.info(String.format(
								"Added %d tokens to attribute vector", i));
					}
				}

				L.info("Creating ARFF dataset with num attributes: " + i);
				// int numTerms = tVector.size();
				int numDocs = ixRd.numDocs();

//				Instances dataset = new Instances("trec2011-twitter-terms",
//						tVector, numDocs);
				ExecutorService exec = Executors.newFixedThreadPool(NUM_WRITE_THREADS);
				Future<Instances> lastFuture = null;
				for (int s = 0; s < numDocs; s += SLICE_SIZE) {
					SliceDocsTransformer sTrans = new SliceDocsTransformer(s,
							numDocs, tVector, ixRd, arffPath);

					lastFuture = exec.submit(sTrans);
					// Instances sInsts = sTrans.call();
					// Enumeration instEnum = sInsts.enumerateInstances();
					// while (instEnum.hasMoreElements()) {
					// Instance inst = (Instance) instEnum.nextElement();
					// dataset.add(inst);
					// inst.setDataset(dataset);
					// }
				}

				lastFuture.get();
				
				exec.shutdown();
				while(!exec.isTerminated()){
					Thread.sleep(1000);
				}
				
			} finally {
				ixRd.close();
			}
		} catch (Exception ex) {
			L.error(ex, ex);
		} finally {
			try {
				notifyStream.flush();
				notifyStream.close();
			} catch (IOException ignored) {
				L.error(ignored, ignored);
			}

			System.setErr(errOrig);
		}
	}

}
