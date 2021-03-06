package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCount extends Configured implements Tool {

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();
		private String[] fieldNames = {"user_id", "event_type", "event_timestamp", "city", "vin", "vehicle_condition", "year", "make", "model", "trim", "body_style", "cab_style", "price", "mileage", "free_carfax_report", "features"};

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			String line = value.toString();
			String[] splitLine = line.split("\t");
			for(int i = 0; i < splitLine.length; i++) {
				if(!fieldNames[i].equals("event_timestamp") && !fieldNames[i].equals("mileage") && !fieldNames[i].equals("price") && !fieldNames[i].equals("user_id") && !fieldNames[i].equals("vin")) {
					// Split up features by ':'
					if(fieldNames[i].equals("features")) {
						String[] featureSplit = splitLine[i].split(":");
						for(int j = 0; j < featureSplit.length; j++) {
							String comb = "feature:" + featureSplit[j];
							word.set(comb);
							context.write(word, Utils.WRITABLE_ONE);
							context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
						}
					} 
					// All valid cases for desired output except features
					else {
						String comb = fieldNames[i] + ":" + splitLine[i];
						word.set(comb);
						context.write(word, Utils.WRITABLE_ONE);
						context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
					}
				}
			}
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (LongWritable value : values) {
				sum += value.get();
			}
			// Emit the total count for the word.
			context.write(key, new LongWritable(sum));
		}
	}

	/**
	 * The run method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "WordCount");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordCount.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileInputFormat.addInputPath(job, new Path(appArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new WordCount(), args);
		System.exit(res);
	}

}
