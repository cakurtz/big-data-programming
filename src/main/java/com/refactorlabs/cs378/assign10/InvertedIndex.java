package com.refactorlabs.cs378.assign10;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Inverted Index application for Spark.
 */
public class InvertedIndex {
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {
            // Load the input data
            JavaRDD<String> input = sc.textFile(inputFilename);

            // Split the input into words with a list of where it occurs
            FlatMapFunction<String, Tuple2<String, List<String>>> splitFunction =
                    new FlatMapFunction<String, Tuple2<String, List<String>>>() {
                        @Override
                        public Iterator<Tuple2<String, List<String>>> call(String line) throws Exception {
                            StringTokenizer tokenizer = new StringTokenizer(line);
                            List<Tuple2<String, List<String>>> tupleList = Lists.newArrayList();
                            String key = "";
                            boolean keyIsSet = false;

                            // For each word in the input line, emit that word.
                            while (tokenizer.hasMoreTokens()) {
                                if(keyIsSet) {
                                	   List<String> chapterList = Lists.newArrayList();
                                	   chapterList.add(key);
                                    tupleList.add(new Tuple2<String, List<String>>(tokenizer.nextToken(), chapterList));
                                }
                                else {
                                	   key = tokenizer.nextToken();
                                	   keyIsSet = true;
                                }
                            }
                            
                            return tupleList.iterator();
                        }
                    };

            // Transform into punctuation-free word and list
            PairFunction<Tuple2<String, List<String>>, String, List<String>> addCountFunction =
                    new PairFunction<Tuple2<String, List<String>>, String, List<String>>() {
						@Override
						public Tuple2<String, List<String>> call(Tuple2<String, List<String>> t) throws Exception {
							String cleanKey = removePunctuation(t._1.toLowerCase());
							return new Tuple2<String, List<String>>(cleanKey, t._2);
						}
                    };

            // Combine the lists 
            Function2<List<String>, List<String>, List<String>> sumFunction =
                    new Function2<List<String>, List<String>, List<String>>() {
                        @Override
                        public List<String> call(List<String> list1, List<String> list2) throws Exception {
                            List<String> combined = Lists.newArrayList();
                            for(int i = 0; i < list1.size(); i++)
                                combined.add(list1.get(i));
                            for(int i = 0; i < list2.size(); i++)
                                combined.add(list2.get(i));
							combined = removeDuplicatesAndSort(combined);
                            return combined;
                        }
                    };

            JavaRDD<Tuple2<String, List<String>>> words = input.flatMap(splitFunction);
            JavaPairRDD<String, List<String>> wordsWithCount = words.mapToPair(addCountFunction);
            JavaPairRDD<String, List<String>> counts = wordsWithCount.reduceByKey(sumFunction);
            JavaPairRDD<String, List<String>> ordered = counts.sortByKey();

            // Save the word count to a text file (initiates evaluation)
            ordered.saveAsTextFile(outputFilename);
        } finally {
            // Shut down the context
            sc.stop();
        }
	}
	
	/*
	 * Removes all punctuation from the front and back of a word
	 */
	private static String removePunctuation(String word) {
		StringBuilder result = new StringBuilder(word);
		while(result.charAt(0) < 'A' || 
			  (result.charAt(0) > 'Z' && 
			  result.charAt(0) < 'a') || 
			  result.charAt(0) > 'z') {
			result.deleteCharAt(0);
		}
		while(result.charAt(result.length() - 1) < 'A' || 
			  (result.charAt(result.length() - 1) > 'Z' && 
			  result.charAt(result.length() - 1) < 'a') || 
			  result.charAt(result.length() - 1) > 'z') {
			result.deleteCharAt(result.length() - 1);
		}
		return result.toString();
	}
	
	/*
	 * Takes a list as input and removes duplicates and then sorts based on
	 * book (string), then chapter(int), then verse (int)
	 */
	private static List<String> removeDuplicatesAndSort(List<String> chapters){
		List<String> result = Lists.newArrayList();
		
		// Remove duplicates and sort on book
		Set<String> removeDups = new HashSet<String>(chapters);
		List<String> tempList = Lists.newArrayList();
		tempList.addAll(removeDups);
		Collections.sort(tempList);
		
		// Sort by chapter and verse
		for(String s : tempList) {
			if(result.isEmpty())
				result.add(s);
			else {
				boolean added = false;
				String[] ssplit = s.split(":");
				String currentBook = ssplit[0];
				Integer currentChapter = new Integer(ssplit[1]);
				Integer currentVerse = new Integer(ssplit[2]);
				
				for(int i = 0; i < result.size(); i++) {
					String[] stsplit = result.get(i).split(":");
					String otherBook = stsplit[0];
					Integer otherChapter = new Integer(stsplit[1]);
					Integer otherVerse = new Integer(stsplit[2]);
					
					if(currentBook.equals(otherBook)) {
						if(currentChapter.equals(otherChapter)) {
							if(currentVerse < otherVerse) {
								if(i == 0)
									result.add(0, s);
								else
									result.add(i, s);
								added = true;
								break;
							}
						} else {
							if(currentChapter < otherChapter) {
								if(i == 0)
									result.add(0, s);
								else
									result.add(i, s);
								added = true;
								break;
							} 
						}
					}
				}
				
				// Add to end if not added 
				if(!added) {
					result.add(s);
				}
			}
		}
		return result;
	}

}
