package com.refactorlabs.cs378.assign3;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count, number of paragraphs a word appears in,
 * computes the mean, and computes the variance.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Base word count program by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class InvertedIndex extends Configured implements Tool {

	/**
	 * The Map class for word inverted index.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the inverted index example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, MapWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			// Example of structure - Map<To:email, Map<1, messageID>>
			Map<String, Map<LongWritable, Text>> messageInfo = 
					new HashMap<String, Map<LongWritable, Text>>();
			// Set to be the first entry in the map for an email
			Map<LongWritable, Text> result = null;  
			String messageID = null; // Stores the messageID
			
			// Flags for indication that a field has already been seen 
			boolean messageIDSet = false, 
					fromSet = false, 
					toSet = false, 
					ccSet = false, 
					bccSet = false;

			// For each word, see if it is relevant to us otherwise move on
			// If a needed header, parse for all emails until next header
			// Designed for holding multiple emails
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				
				// MessageID
				if(word.toString().equals("Message-ID:") && !messageIDSet) {
					word.set(tokenizer.nextToken());
					messageID = word.toString();
					result = new HashMap<LongWritable, Text>();
					result.put(new LongWritable(1L), new Text(messageID));
					messageIDSet = true;
				}
				
				// From
				else if(word.toString().equals("From:") && !fromSet) {
					word.set(tokenizer.nextToken());
					while(!word.toString().equals("To:")) {
						mapInput(messageInfo, result, messageID, "From:");
						word.set(tokenizer.nextToken());
					}
					fromSet = true;
				}
				
				// To
				if(word.toString().equals("To:") && ! toSet) {
					word.set(tokenizer.nextToken());
					while(!word.toString().equals("Subject:")) {
						mapInput(messageInfo, result, messageID, "To:");
						word.set(tokenizer.nextToken());
					}
					toSet = true;
				}
				
				// CC - Optional
				if(word.toString().equals("Cc:") && ! ccSet) {
					word.set(tokenizer.nextToken());
					while(!word.toString().equals("Mime-Version:")) {
						mapInput(messageInfo, result, messageID, "Cc:");
						word.set(tokenizer.nextToken());
					}
					ccSet = true;
				}
				
				// BCC - Optional
				if(word.toString().equals("Bcc:") && ! bccSet) {
					word.set(tokenizer.nextToken());
					while(!word.toString().equals("X-From:")) {
						mapInput(messageInfo, result, messageID, "Bcc:");
						word.set(tokenizer.nextToken());
					}
					bccSet = true;
				}
				
				// End of needed header information, break out 
				if(word.toString().equals("X-From:")) {
					break;
				}
			}
			
			// Output entire contents of map
			MapWritableHelper resultsWritable;
			for(String entry : messageInfo.keySet()) {
				word = new Text(cleanKey(entry));
				resultsWritable = new MapWritableHelper();
				resultsWritable.putAll(messageInfo.get(entry));
				context.write(word, resultsWritable);
				context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
		
		/* 
		 * This method removes possible commas at the end of a key
		 */
		private String cleanKey(String key) {
			String result = key;
			if(key.charAt(key.length() - 1) == ',') {
				result = result.substring(0, result.length() - 1);
			}
			return result;
		}
		
		/*
		 * This method puts input into the result map to be written out of the 
		 * mapper upon completion
		 */
		private void mapInput(Map<String, Map<LongWritable, Text>> messageInfo, Map<LongWritable, Text> result, String messageID, String version) {
			String keyString = version + word.toString();
			if(messageInfo.containsKey(keyString)) {
				Map<LongWritable, Text> temp = new HashMap<LongWritable, Text>();
				temp.putAll(messageInfo.get(keyString));
				Integer size = temp.size() + 1;
				temp.put(new LongWritable((Long) size.longValue()), new Text(messageID));
				messageInfo.put(keyString, temp);
			}
			else {
				messageInfo.put(keyString, result);
			}
		}
	}
	
	/* 
	 * This class extends functionality of the MapWritable class by adding a
	 * toString() method for ease of use 
	 */
	public static class MapWritableHelper extends MapWritable {
	
		public MapWritableHelper() {
		}
		
		@Override
		public String toString() {
			String result = "";
			for (Object value : this.keySet()) {
				result += this.get(value).toString() + " ";
			}
			return result;
		}
	}

	/**
	 * The Reduce class for inverted index.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the inverted index example.
	 */
	public static class ReduceClass extends Reducer<Text, MapWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			String finalResult = "";

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Turn the results into a string
			for(MapWritable value : values) {
				for (Writable keyNumber : value.keySet()) {
					finalResult += value.get(keyNumber).toString() + ",";
				}
			}
			// Remove last comma
			if(finalResult.length() > 0) {
				finalResult = finalResult.substring(0, finalResult.length() - 1);
			}
			
			// Emit the final results.
			context.write(key, new Text(finalResult));
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

		Job job = Job.getInstance(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritableHelper.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

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
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}

}
