package ca.uwaterloo.hadoop.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpSeqFilesFolder {
	private static Logger L = LoggerFactory.getLogger("DumpSeqFilesFolder");

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

//		int pHeapSize = 5000;

		String seqPath = args[0];

		PrintStream out = new PrintStream(new FileOutputStream(
						 seqPath + File.separator + "assoc.csv"), 
						true, "UTF-8");
		seqPath += File.separator + "frequentpatterns";
		


		FileSystem fs = FileSystem.get(new Configuration());
		Path file = new Path(seqPath);
		if (!fs.exists(file)) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}

		CorpusReader<Writable, TopKStringPatterns> stream = new CorpusReader<Writable, TopKStringPatterns>(
				file, fs, "part.*");
//		HashMap<String, TopKStringPatterns> merged = new HashMap<String, TopKStringPatterns>();
		try {
			
			Pair<Writable, TopKStringPatterns> p;
			while ((p = stream.next()) != null) {
				
				Writable first = p.getFirst();
				TopKStringPatterns second = p.getSecond();
				
				if (second.getPatterns().size() == 0) {
					L.debug("Zero patterns for the feature: {}",
							first.toString());
				} else {
					L.trace(first.toString() + "\t" + second.toString());
//					if(!merged.containsKey(first.toString())){
//						merged.put(first.toString(), new TopKStringPatterns());
//					}
//					TopKStringPatterns m = merged.get(first.toString());
//					m = m.merge(second, pHeapSize);
//					merged.put(first.toString(), m);
//					System.err.println(m.getPatterns());
					
					out.println(first.toString() + "\t" + second.getPatterns().toString());
					
				}
			}

			
			
		} catch (Exception ex) {
			L.error(ex.getMessage(), ex);
		} finally {
			
//			for(Entry<String, TopKStringPatterns> e: merged.entrySet()){
//				out.println(e.getKey().toString() + "\t" + e.getValue().toString());
//			}
			out.flush();
			out.close();
			stream.close();
		}
	}

}
