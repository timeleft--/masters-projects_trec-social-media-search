package ca.uwaterloo.trec2012;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import uwaterloo.util.NotifyStream;
import weka.associations.AbstractAssociator;
import weka.associations.Apriori;
import weka.core.Instances;
import weka.core.converters.ArffLoader.ArffReader;

public class TwitterFreqItemsets {
	private static Logger L = Logger.getLogger("TwitterFreqItemsets");

	public static void main(String[] args) throws IOException {
		PrintStream errOrig = System.err;
		NotifyStream notifyStream = new NotifyStream(errOrig, "LuceneToArff");
		try {
			System.setErr(new PrintStream(notifyStream));

			String arffPath = "D:\\datasets\\twitter-trec2011\\arff-1";
			String evalPath = "D:\\datasets\\twitter-trec2011\\assoc";
			
			File arffDir = new File(arffPath);
			for (File arffFile : arffDir.listFiles()) {
				AbstractAssociator assoc = new Apriori();
				ArffReader arffReader = new ArffReader(new FileReader(arffFile));
				L.info("Reading: " + arffFile.getAbsolutePath());
				Instances data = arffReader.getData();
				L.info("Loaded: " + arffFile.getAbsolutePath());
				assoc.buildAssociations(data);
				L.info("Created associations from: " + arffFile.getAbsolutePath());
				FileUtils.writeStringToFile(FileUtils.getFile(evalPath, arffFile.getName()), assoc.toString());
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
