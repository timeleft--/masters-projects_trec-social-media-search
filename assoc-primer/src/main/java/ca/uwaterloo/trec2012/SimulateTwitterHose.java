package ca.uwaterloo.trec2012;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.twitter.corpus.data.HtmlStatus;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class SimulateTwitterHose {
	private static Logger L = Logger.getLogger("SimulateTwitterHose");

	

	public static class HtmlStatusBlockReader {

		private final SequenceFile.Reader reader;

		private final PairOfLongString key = new PairOfLongString();
		private final HtmlStatus value = new HtmlStatus();

		public HtmlStatusBlockReader(Path path, FileSystem fs)
				throws IOException {
			Preconditions.checkNotNull(path);
			Preconditions.checkNotNull(fs);
			reader = new SequenceFile.Reader(fs, path, fs.getConf());
		}

		/**
		 * Returns the next status, or <code>null</code> if no more statuses.
		 */
		public PairOfLongString next() throws IOException {
			if (!reader.next(key, value))
				return null;

			PairOfLongString result= null;
			int httpStatus = value.getHttpStatusCode();
			if(httpStatus >= 400){
				result = next();
			} else {
				result = new PairOfLongString(extractTimestamp(value.getHtml()),
					String.format(
							"%d\t%s\t%d\t%s",
							key.getLeftElement(),
							"\"" + key.getRightElement() + "\"",
							httpStatus,
							"\""
									+ StringEscapeUtils.escapeJava(value
											.getHtml()) + "\""));
			}
			return result;
		}

		public void close() throws IOException {
			reader.close();
		}

		private final Pattern TIMESTAMP_PATTERN = Pattern
				.compile("<span class=\"published timestamp\" data=\"\\{time:'([^']+)'\\}\">");

//		Sun Jan 23 00:00:00 +0000 2011
		private final DateFormat dFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
		
		public Long extractTimestamp(String html) {
			Preconditions.checkNotNull(html);
			Matcher matcher = TIMESTAMP_PATTERN.matcher(html);
			if (!matcher.find()) {
				return null;
			}
			String resString = matcher.group(1);
			
			try {
				return dFmt.parse(resString).getTime();
			} catch (ParseException e) {
				L.error(e, e);
				return null;
			}
		}
	}

	public static class HtmlStatusCorpusReader {
		private final FileStatus[] files;
		private final FileSystem fs;

		private int nextFile = 0;
		private HtmlStatusBlockReader currentBlock = null;

		public HtmlStatusCorpusReader(Path directory, FileSystem fs)
				throws IOException {
			Preconditions.checkNotNull(directory);
			this.fs = Preconditions.checkNotNull(fs);
			if (!fs.getFileStatus(directory).isDir()) {
				throw new IOException("Expecting " + directory
						+ " to be a directory!");
			}

			files = fs.listStatus(directory);

			if (files.length == 0) {
				throw new IOException(directory
						+ " does not contain any files!");
			}
		}

		/**
		 * Returns the next status, or <code>null</code> if no more statuses.
		 */
		public PairOfLongString next() throws IOException {
			if (currentBlock == null) {
				currentBlock = new HtmlStatusBlockReader(
						files[nextFile].getPath(), fs);
				nextFile++;
			}

			PairOfLongString status = null;
			while (true) {
				status = currentBlock.next();
				if (status != null) {
					return status;
				}

				if (nextFile >= files.length) {
					// We're out of files to read. Must be the end of the
					// corpus.
					return null;
				}

				currentBlock.close();
				// Move to next file.
				currentBlock = new HtmlStatusBlockReader(
						files[nextFile].getPath(), fs);
				nextFile++;
			}
		}

		public void close() throws IOException {
			currentBlock.close();
		}
	}

	public static class TwitterHose implements Callable<Void> {

		long clkTick = 1000;
		private HtmlStatusCorpusReader stream;
		private PrintStream out;

		public TwitterHose(HtmlStatusCorpusReader stream, PrintStream out) {
			this.stream = stream;
			this.out = out;
		}

		public Void call() throws Exception {
			long time;
			long nxtTwtTime;

			int cnt = 0;
			PairOfLongString status;
			try {
				status = stream.next();
				time = status.getLeftElement() - clkTick;

				do {
					nxtTwtTime = status.getLeftElement();
					while (time < nxtTwtTime) {
						Thread.sleep(clkTick);
						time += clkTick;
					}

					String csv = status.getRightElement();
					out.println(csv);
				} while ((status = stream.next()) != null);

				cnt++;
				if (cnt % 10000 == 0) {
					L.info(cnt + " statuses read");
				}

			} finally {
				if (stream != null) {
					stream.close();
				}
			}

			L.info(String.format("Total of %s statuses read.", cnt));

			return null;
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		PrintStream errOrig = System.err;
//		NotifyStream notifyStream = new NotifyStream(errOrig, "LuceneToArff");
		try {
//			System.setErr(new PrintStream(notifyStream));

			PrintStream out = new PrintStream(System.out, true, "UTF-8");

			String htmlseqPath = "D:\\datasets\\twitter-trec2011\\html";

			FileSystem fs = FileSystem.get(new Configuration());
			Path file = new Path(htmlseqPath);
			if (!fs.exists(file)) {
				System.err.println("Error: " + file + " does not exist!");
				System.exit(-1);
			}
			
			
			

			HtmlStatusCorpusReader stream = new HtmlStatusCorpusReader(file, fs);
			
			TwitterHose hose = new TwitterHose(stream, out);
			ExecutorService exec = Executors.newSingleThreadExecutor();
			exec.submit(hose).get();

			exec.shutdown();

		} catch (Exception ex) {
			L.error(ex, ex);
		} finally {
//			try {
//				notifyStream.flush();
//				notifyStream.close();
//			} catch (IOException ignored) {
//				L.error(ignored, ignored);
//			}
//
//			System.setErr(errOrig);
		}
	}

}
