package ca.uwaterloo.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CorpusReader<K extends Writable, V extends Writable> {
	private static final Logger L = LoggerFactory.getLogger("CorpusReader");
	private final FileStatus[] files;
	private final FileSystem fs;

	private int nextFile = 0;
	private SequenceFileIterator<K, V> currentBlock = null;

	public CorpusReader(Path directory, FileSystem fs) throws IOException {
		Preconditions.checkNotNull(directory);
		this.fs = Preconditions.checkNotNull(fs);
		if (!fs.getFileStatus(directory).isDir()) {
			throw new IOException("Expecting " + directory
					+ " to be a directory!");
		}

		files = fs.listStatus(directory);

		if (files.length == 0) {
			throw new IOException(directory + " does not contain any files!");
		}
	}

	/**
	 * Returns the next status, or <code>null</code> if no more statuses.
	 */
	@SuppressWarnings("unchecked")
	public Pair<K, V> next() throws IOException {
		try {
			if (currentBlock == null) {
				currentBlock = (SequenceFileIterator<K, V>) new SequenceFileIterable<K, V>(
						files[nextFile].getPath(), true, fs.getConf())
						.iterator();
				nextFile++;
			}

			while (true) {

				if (currentBlock.hasNext()) {
					return currentBlock.next();
				}

				if (nextFile >= files.length) {
					// We're out of files to read. Must be the end of the
					// corpus.
					return null;
				}

				currentBlock.close();
				// Move to next file.
				currentBlock = (SequenceFileIterator<K, V>) new SequenceFileIterable<K, V>(
						files[nextFile].getPath(), true, fs.getConf())
						.iterator();
				nextFile++;
			}
		} catch (IllegalStateException ex) {
			L.error(ex.getMessage(), ex);
			return null;
		}
	}

	public void close() throws IOException {
		currentBlock.close();
	}
}
