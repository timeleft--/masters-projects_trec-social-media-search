package org.apache.mahout.freqtermsets;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.mahout.common.Pair;
import org.apache.mahout.freqtermsets.ParallelFPGrowthReducer.IteratorAdapter;
import org.apache.mahout.freqtermsets.convertors.ContextStatusUpdater;
import org.apache.mahout.freqtermsets.convertors.ContextWriteOutputCollector;
import org.apache.mahout.freqtermsets.convertors.integer.IntegerStringOutputConverter;
import org.apache.mahout.freqtermsets.convertors.string.TopKStringPatterns;
import org.apache.mahout.freqtermsets.fpgrowth.FPGrowth;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntObjectHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uwaterloo.twitter.TokenIterator.LatinTokenIterator;

import com.google.common.collect.Lists;
import com.twitter.corpus.data.CSVTweetRecordReader;

import edu.umd.cloud9.io.pair.PairOfLongs;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PruningTest {
	protected static final Logger LOG = LoggerFactory
			.getLogger(PruningTest.class);

	private static String countIn = "/Users/yia/Dropbox/fis/assoc-mr_chuncks-15min_day1/1295740800000/" + 
			"1295741700000";
			//"/Users/yia/Dropbox/fis/assoc-mr_chuncks-5min_day1/1295740800000/1295741100000/";
	private static String csvIn = "/Users/yia/Dropbox/tweets_csv_17hr/";

	private static String outPath = "/Users/yia/Dropbox/fis/assoc-mr_chuncks-15min_day1/1295740800000/" + 
			"1295741700000";
//			"/Users/yia/Dropbox/fis/assoc-mr_chuncks-5min_day1/1295740800000/1295741100000/";

	private static boolean prependUserName = true;
	private static boolean repeatHashTag = false;

	private static Configuration conf = new Configuration();
	private static List<Pair<String, Long>> flist;
	private static OpenIntObjectHashMap<String> featureReverseMap = new OpenIntObjectHashMap<String>();
	private static OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
	private static IntArrayList returnableFeatures = new IntArrayList();
	private static TransactionTree cTree = new TransactionTree();

	@BeforeClass
	public static void beforeClass() throws IOException, InterruptedException {
		flist = PFPGrowth.readFList(countIn, 2, 3, 100, conf);
		int ix = 0;
		for (Pair<String, Long> e : flist) {
			featureReverseMap.put(ix, e.getFirst());
			fMap.put(e.getFirst(), ix);
			returnableFeatures.add(ix);
			++ix;
		}

		CSVTweetRecordReader csvReader = new CSVTweetRecordReader();
		Path[] csvPaths = new Path[3];
		csvPaths[0] = new Path(csvIn, "1295740800000/1295741100000");
		csvPaths[1] = new Path(csvIn, "1295740800000/1295741400000");
		csvPaths[2] = new Path(csvIn, "1295740800000/1295741700000");
		CombineFileSplit inputSplit = new CombineFileSplit(csvPaths,
				new long[] { -1L });
		csvReader.initialize(inputSplit, conf);

		while (csvReader.nextKeyValue()) {
			PairOfLongs key = csvReader.getCurrentKey();
			long id = key.getLeftElement();
			long timestamp = key.getRightElement();

			PairOfStrings value = csvReader.getCurrentValue();
			String screenName = value.getLeftElement();
			String tweet = value.getRightElement();

			String inputStr;
			// for (String item : items) {
			if (prependUserName) {
				inputStr = "@" + screenName + ": " + tweet;
			} else {
				inputStr = tweet;
			}

			OpenIntHashSet itemSet = new OpenIntHashSet();

			LatinTokenIterator items = new LatinTokenIterator(inputStr);
			items.setRepeatHashTag(repeatHashTag);
			while (items.hasNext()) {
				String item = items.next();
				if (fMap.containsKey(item) && !item.trim().isEmpty()) {
					itemSet.add(fMap.get(item));
				}
			}

			IntArrayList itemArr = new IntArrayList(itemSet.size());
			itemSet.keys(itemArr);
			// YA: why is sort needed here? won't group dependent transactions
			// (below) become
			// just monotonically increasing lists of items because of this?
			itemArr.sort();

			cTree.addPattern(itemArr, 1L);
		}

	}

	@Rule
	public TestName name = new TestName();

	Reducer.Context context;
	Writer resultWr;

	@Before
	public void setup() throws IOException, InterruptedException {
		context = mock(Reducer.Context.class);
		doAnswer(new Answer() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				LOG.info(invocation.getArguments()[0].toString());
				return null;
			}
		}).when(context).setStatus(anyString());
		
		resultWr = Channels.newWriter(
				FileUtils.openOutputStream(
						new File(outPath, name.getMethodName() + ".csv"))
						.getChannel(), "UTF-8");
		
		doAnswer(new Answer() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				resultWr.append(args[0] + "\t" + args[1] + "\n");
				return null;
			}
		}).when(context).write(any(Object.class), any(Object.class));
	}

	public void tearDown() throws IOException {
		resultWr.flush();
		resultWr.close();
	}

	
	public void testPruneTransactionTree() {
		
		
	}
	
	@Test
	public void testPruneFPTree() throws IOException {
		List<Pair<Integer, Long>> localFList = Lists.newArrayList();
		for (Entry<Integer, MutableLong> fItem : cTree.generateFList()
				.entrySet()) {
			localFList.add(new Pair<Integer, Long>(fItem.getKey(), fItem
					.getValue().toLong()));
		}

		Collections.sort(localFList,
				new CountDescendingPairComparator<Integer, Long>());

		FPGrowth<Integer> fpGrowth = new FPGrowth<Integer>();
		fpGrowth.generateTopKFrequentPatterns(
//				new IteratorAdapter(cTree.iterator()),
				cTree,
				localFList,
				2,
				50,
				new HashSet<Integer>(returnableFeatures.toList()),
				new IntegerStringOutputConverter(
						new ContextWriteOutputCollector<IntWritable, TransactionTree, Text, TopKStringPatterns>(
								context), featureReverseMap, 0/*
															 * ,
															 * superiorityRatio
															 */, repeatHashTag),
				new ContextStatusUpdater<IntWritable, TransactionTree, Text, TopKStringPatterns>(
						context), -1, -1); // those will not be used as long as
											// there is something in the
											// returnable features
	}
}
