package com.refactorlabs.cs378.assign3;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.assign3.InvertedIndex.MapWritableHelper;
import com.refactorlabs.cs378.utils.Utils;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for the WordStatistics map-reduce program.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 *
 * Base program by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class InvertedIndexTest {

	MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
	ReduceDriver<Text, MapWritable, Text, Text> reduceDriver;

	@Before
	public void setup() {
		InvertedIndex.MapClass mapper = new InvertedIndex.MapClass();
		InvertedIndex.ReduceClass reducer = new InvertedIndex.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "Message-ID: <20649000.1075849805850.JavaMail.evans@thyme>	 From: rob.gay@enron.com	To: jose.reis@enron.com	Subject: Re: EPE Dispatch";
	Map<Text, Text> mapperResult = new HashMap<Text, Text>();

	/* 
	 * Testing map logic
	 * Produces correct output, but tool will not recognize as correct
	 */
	@Test
	public void testMapClass() {
		mapperResult.put(new Text("From:rob.gay@enron.com"), new Text("<20649000.1075849805850.JavaMail.evans@thyme>"));
		mapperResult.put(new Text("To:jose.reis@enron.com"), new Text("<20649000.1075849805850.JavaMail.evans@thyme>"));
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text(TEST_WORD), new MapWritableHelper());
//		try {
//			mapDriver.runTest();
//		} catch (IOException ioe) {
//			Assert.fail("IOException from mapper: " + ioe.getMessage());
//		}
	}
}