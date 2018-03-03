package com.refactorlabs.cs378.assign4;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count, number of paragraphs a word appears in,
 * the minimum and maximum number of times it appears in a paragraph, 
 * computes the mean, and computes the variance using the Avro package.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Base word count program by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatisticsA extends Configured implements Tool {

	/**
	 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word statistics example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, 
											   Text, AvroValue<WordStatisticsData>> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();
		private final int FOOTNOTE_SIZE = 5;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			Map<String, Long> word_count = new HashMap<String, Long>();
			long count = 0; // Keeps track of the number of words in the paragraph

			// For each distinct word in the input line, add an entry in total word count map
			// If it already exists, increment the word's value by 1
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				String current = word.toString().toLowerCase();
				current = parseWord(current);
				
				// Special case where footnotes with 3 digits and brackets exist in word
				// Broken into two words
				current = checkForFootnote(word_count, current);
				
				// Special case when word has -- and other punctuation inside
				// Typically broken up into two words 
				current = checkForInnerPunctuation(word_count, current);
				
				// Normal case - add word to map if not already there,
				// increment the value otherwise
				if(!word_count.containsKey(current)) {
					word_count.put(current, (long) 1);
				}
				else {
					long incr = word_count.get(current) + 1;
					word_count.put(current, incr);
				}
				count++;
			}
			
			// Add overall paragraph statistics to map
			word_count.put("paragraph length", count);
			
			// Output entire contents of word count map
			for(String entry : word_count.keySet()) {
				long temp = 0;
				word = new Text(entry);
				temp = word_count.get(entry);
				WordStatisticsData.Builder stats = generateBuilder(temp);
				context.write(word, new AvroValue<WordStatisticsData>(stats.build()));
				context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
		
		/*
		 * Builds the builder for the WordStatisticsData Avro object
		 */
		private WordStatisticsData.Builder generateBuilder(long count){
			WordStatisticsData.Builder stats = WordStatisticsData.newBuilder();
			stats.setDocumentCount(1L);
			stats.setTotalCount(count);
			stats.setMin(count);
			stats.setMax(count);
			stats.setSumOfSquares(count*count);
			stats.setMean(0.0);
			stats.setVariance(0.0);
			return stats;
		}
		
		/* 
		 * Parses the word to ensure the front and back are clear of punctuation
		 */
		private String parseWord(String word) {
			String result = word;
			
			// Removes all relevant punctuation in front of the word
			while(result.charAt(0) == '"' || result.charAt(0) == '_' || 
				  result.charAt(0) == '-' || result.charAt(0) == '"') {
				result = result.substring(1);
			}
			
			// Removes all relevant punctuation at the end of the word
			while(result.charAt(result.length() - 1) == '.' || 
				  result.charAt(result.length() - 1) == ',' || 
				  result.charAt(result.length() - 1) == ';' || 
				  result.charAt(result.length() - 1) == '"' || 
				  result.charAt(result.length() - 1) == '!' || 
				  result.charAt(result.length() - 1) == '?' || 
				  result.charAt(result.length() - 1) == '_' || 
				  result.charAt(result.length() - 1) == '-' || 
				  result.charAt(result.length() - 1) == ':') {
				result = result.substring(0, result.length()-1);
			}
			return result;
		}
		
		/* 
		 * Checks the string for internal footnotes.
		 * Returns the first word and adds the footnote to the map.
		 */
		private String checkForFootnote(Map<String, Long> word_count, String current) {
			if(current.charAt(current.length()-1) == ']') {
				String holder = current.substring(current.length() - 
								FOOTNOTE_SIZE, current.length());
				holder = parseWord(holder);
				if(!word_count.containsKey(holder)) {
					word_count.put(holder, (long) 1);
				}
				else {
					long incr = word_count.get(holder) + 1;
					word_count.put(holder, incr);
				}
				current = current.substring(0, current.length() - FOOTNOTE_SIZE);
				current = parseWord(current);
			}
			return current;
		}
		
		/* 
		 * Checks the string for internal punctuation and breaks up into two words.
		 * Returns the first word and adds the second to the map.
		 */
		private String checkForInnerPunctuation(Map<String, Long> word_count, String current) {
			for(int i = 0; i < current.length(); i++) {
				if(current.charAt(i) == '-' && current.charAt(i + 1) == '-') {
					String holder = current.substring(i + 1, current.length());
					holder = parseWord(holder);
					if(!word_count.containsKey(holder)) {
						word_count.put(holder, (long) 1);
					}
					else {
						long incr = word_count.get(holder) + 1;
						word_count.put(holder, incr);
					}
					current = current.substring(0, i - 1);
					current = parseWord(current);
				}
			}
			return current;
		}
	}

	/**
	 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word statistics example.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, 
												   Text, AvroValue<WordStatisticsData>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			long paragraph_count = 0L;
			Long min = null;
			long max = 1L;
			Double count_squared = new Double(0.0);
			boolean minSet = false;

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (AvroValue<WordStatisticsData> value : values) {
				paragraph_count += value.datum().getDocumentCount();
				sum += value.datum().getTotalCount();
				count_squared += value.datum().getSumOfSquares();
				if(value.datum().getMax() > max) {
					max = value.datum().getMax();
				}
				// Set min 
				if(minSet) {
					if(value.datum().getMin() < min.longValue()) {
						min = value.datum().getMin();
					}
				}
				else {
					min = value.datum().getMin();
					minSet = true;
				}
			}
			
			// Compute mean
			double mean = 0.0;
			mean = ((double) sum) / ((double) paragraph_count);
			
			// Compute variance
			double variance = 0.0;
			variance = (count_squared / paragraph_count) - (mean * mean);
			
			WordStatisticsData.Builder stats = 
					generateBuilder(paragraph_count, sum, min, max, 
								    count_squared.longValue(), mean, variance);
			// Emit the total count for the word.
			context.write(key, new AvroValue<WordStatisticsData>(stats.build()));
		}
		
		/*
		 * Builds the builder for the WordStatisticsData Avro object
		 */
		private WordStatisticsData.Builder generateBuilder(long paragraph_count, 
				long sum, Long min, long max, long count_squared, double mean, 
				double variance){
			WordStatisticsData.Builder stats = WordStatisticsData.newBuilder();
			stats.setDocumentCount(paragraph_count);
			stats.setTotalCount(sum);
			stats.setMin(min.longValue());
			stats.setMax(max);
			stats.setSumOfSquares(count_squared);
			stats.setMean(mean);
			stats.setVariance(variance);
			return stats;
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

		Job job = Job.getInstance(conf, "WordStatisticsA");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatisticsA.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsData.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());
		AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

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
		int res = ToolRunner.run(new WordStatisticsA(), args);
		System.exit(res);
	}

}
