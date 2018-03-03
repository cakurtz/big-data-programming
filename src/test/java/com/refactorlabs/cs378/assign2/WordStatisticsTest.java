package com.refactorlabs.cs378.assign2;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for the WordStatistics map-reduce program.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 *
 * Base program by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatisticsTest {

	MapDriver<LongWritable, Text, Text, WordStatisticsWritable> mapDriver;
	ReduceDriver<Text, WordStatisticsWritable, Text, WordStatisticsWritable> reduceDriver;

	@Before
	public void setup() {
		WordStatistics.MapClass mapper = new WordStatistics.MapClass();
		WordStatistics.ReduceClass reducer = new WordStatistics.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "Yadayada";

	/* 
	 * Test combiner results 
	 */
	@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text(TEST_WORD.toLowerCase()), new WordStatisticsWritable(1,1.0,1.0));
		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	/* 
	 * Testing mathematical equations in reducer 
	 */
	@Test
	public void testReduceClass() {
		List<WordStatisticsWritable> valueList = Lists.newArrayList(new WordStatisticsWritable(1,1.0,2.0), 
				new WordStatisticsWritable(5,4.0,3.0), new WordStatisticsWritable(1,1.0,4.0));
		reduceDriver.withInput(new Text(TEST_WORD), valueList);
		long paragraph_count = 7;
		double mean = 0.8571428571428571;
		double variance = 0.5510204081632655;
		reduceDriver.withOutput(new Text(TEST_WORD), new WordStatisticsWritable(paragraph_count, mean, variance));
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}

	}
}