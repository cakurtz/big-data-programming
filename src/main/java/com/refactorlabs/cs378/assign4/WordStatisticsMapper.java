package com.refactorlabs.cs378.assign4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.refactorlabs.cs378.utils.Utils;

/*
 * Used only for the test class WordStatisticsMapperTest and does not calculate overall paragraph
 * statistics like the full version does
 */
public class WordStatisticsMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>>{
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
		//word_count.put("paragraph length", count);
		
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
