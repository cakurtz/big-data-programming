package com.refactorlabs.cs378.assign4;

import junit.framework.Assert;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit test of WordCountMapper.
 *
 * Demonstrates how AvroSerialization is configured so that MRUnit code
 * understands how to serialize the mapper output and the expected output
 * for comparison.
 *
 * Configuring the AvroSerialization solution was found here:
 * http://stackoverflow.com/questions/15230482/mrunit-with-avro-nullpointerexception-in-serialization
 *
 * Author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatisticsMapperTest {

    MapDriver<LongWritable, Text, Text, AvroValue<WordStatisticsData>> mapDriver;

    @Before
    public void setup() {
        WordStatisticsMapper mapper = new WordStatisticsMapper();

        mapDriver = MapDriver.newMapDriver(mapper);

        // Copy over the default io.serializations. If you don't do this then you will
        // not be able to deserialize the inputs to the mapper
        String[] strings = mapDriver.getConfiguration().getStrings("io.serializations");
        String[] newStrings = new String[strings.length +1];
        System.arraycopy( strings, 0, newStrings, 0, strings.length );
        newStrings[newStrings.length-1] = AvroSerialization.class.getName();

        // Now you have to configure AvroSerialization by specifying the value writer schema.
        mapDriver.getConfiguration().setStrings("io.serializations", newStrings);
        mapDriver.getConfiguration().setStrings("avro.serialization.value.writer.schema",
                WordStatisticsData.SCHEMA$.toString(true));

        // If the mapper outputs an AvroKey,
        // we need to configure AvroSerialization by specifying the key writer schema.
//		mapDriver.getConfiguration().setStrings("avro.serialization.key.writer.schema",
//				Schema.create(Schema.Type.STRING).toString(true));
    }

    private static final String TEST_WORD = "Yadayada";

    @Test
    public void testMapClass() {
        // Create the expected output value.
        WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
        builder.setDocumentCount(1L);
		builder.setTotalCount(1L);
		builder.setMin(1L);
		builder.setMax(1L);
		builder.setSumOfSquares(1L);
		builder.setMean(0.0);
		builder.setVariance(0.0);

        mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
        mapDriver.withOutput(new Text(TEST_WORD.toLowerCase()), new AvroValue<WordStatisticsData>(builder.build()));
        try {
            mapDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }

}

