package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * User Sessions application for Spark.
 */
public class UserSessions {
	private static final int USERID_INDEX = 0;
	private static final int EVENT_TYPE_INDEX = 1;
	private static final int EVENT_TIMESTAMP_INDEX = 2;
	private static final int CITY_INDEX = 3;
	private static final int VIN_INDEX = 4;
	private static final double RANDOM_SAMPLE = 0.1;
	private static Random rand = new Random();
	
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilenameA = args[0];
		String inputFilenameB = args[1];
		String outputFilename = args[2];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(UserSessions.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Set up accumulators
		Accumulator<Integer> eventsBeforeFiltering = sc.intAccumulator(0);
		Accumulator<Integer> eventsAfterFiltering = sc.accumulator(0);
		Accumulator<Integer> sessions = sc.accumulator(0);
		Accumulator<Integer> showerSessions = sc.accumulator(0);
		Accumulator<Integer> showerSessionsFiltered = sc.accumulator(0);

		try {
            // Load the input data
            JavaRDD<String> inputA = sc.textFile(inputFilenameA);
            JavaRDD<String> inputB = sc.textFile(inputFilenameB);

            // Split the input into user sessions with a list of Event
            Function<String, Tuple2<Tuple2<String, String>, List<Event>>> splitFunction =
                    new Function<String, Tuple2<Tuple2<String, String>, List<Event>>>() {
                        @Override
                        public Tuple2<Tuple2<String, String>, List<Event>> call(String line) throws Exception {
                            String[] event = line.split("	");
                            String[] splitType = event[EVENT_TYPE_INDEX].split(" ");

                            String userId = event[USERID_INDEX];
                            String city = event[CITY_INDEX];
                            
                            Event e = new Event();
                            e.eventType = splitType[0];
                            String subtype = splitType[1];
                            for(int i = 2; i < splitType.length; i++) {
                                subtype += " " + splitType[i];
                            }
                            e.eventSubType = subtype;
                            e.eventTimestamp = event[EVENT_TIMESTAMP_INDEX];
                            e.vin = event[VIN_INDEX];
                            
                            Tuple2 resultKey = new Tuple2<String, String>(userId, city);
                            List<Event> resultValue = new ArrayList<Event>();
                            resultValue.add(e);
                            
                            return new Tuple2<Tuple2<String, String>, List<Event>>(resultKey, resultValue);
                        }
                    };

            // Transform into key and value
            PairFunction<Tuple2<Tuple2<String, String>, List<Event>>, Tuple2<String, String>, List<Event>> keyValueFunction =
                    new PairFunction<Tuple2<Tuple2<String, String>, List<Event>>, Tuple2<String, String>, List<Event>>() {
						@Override
						public Tuple2<Tuple2<String, String>, List<Event>> call(Tuple2<Tuple2<String, String>, List<Event>> t) throws Exception {
							Tuple2 key = new Tuple2<String, String>(t._1._1, t._1._2);
							return new Tuple2<Tuple2<String, String>, List<Event>>(key, t._2);
						}
                    };

            // Combine the lists 
            Function2<List<Event>, List<Event>, List<Event>> combineFunction =
                    new Function2<List<Event>, List<Event>, List<Event>>() {
                        @Override
                        public List<Event> call(List<Event> list1, List<Event> list2) throws Exception {
                            List<Event> combined = Lists.newArrayList();
                            for(int i = 0; i < list1.size(); i++)
                                combined.add(list1.get(i));
                            for(int i = 0; i < list2.size(); i++)
                                combined.add(list2.get(i));
							combined = removeDuplicatesAndSort(combined);
                            return combined;
                        }
                    };
                    
            // Sample SHOWER sessions - Only use 1 in 10
            Function<Tuple2<Tuple2<String, String>, List<Event>>, Boolean> filterFunction =
                    new Function<Tuple2<Tuple2<String, String>, List<Event>>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<Tuple2<String, String>, List<Event>> tup) throws Exception {
                            List<Event> result = tup._2();
                            boolean foundContactForm = false;
                            boolean foundClick = false;
                            boolean foundShowOrDisplay = false;
                            
                            for(Event e : result) {
                            	   if(e.eventSubType.equals("contact form"))
                            		   foundContactForm = true;
                            	   if(e.eventType.equals("click"))
                            		   foundClick = true;
                            	   if(e.eventType.equals("show") || e.eventType.equals("display"))
                            		   foundShowOrDisplay = true;
                            }
                            
                            eventsBeforeFiltering.add(result.size());
                            sessions.add(1);
                            
                            // SHOWER only - Random Sample (1 in 10)
                            if(!foundContactForm && !foundClick && foundShowOrDisplay) {
                            		showerSessions.add(1);
                            		if(rand.nextDouble() < RANDOM_SAMPLE) {
                            			eventsAfterFiltering.add(result.size());
                            			return true;
                            		}
                            		else {
                            			showerSessionsFiltered.add(1);
                            			return false;
                            		}
                            }
                            
                            eventsAfterFiltering.add(result.size());
                            return true;
                        }
                    };

            JavaRDD<Tuple2<Tuple2<String, String>, List<Event>>> eventSplitA = inputA.map(splitFunction);
            JavaRDD<Tuple2<Tuple2<String, String>, List<Event>>> eventSplitB = inputB.map(splitFunction);
            JavaRDD<Tuple2<Tuple2<String, String>, List<Event>>> combinedEvents = sc.union(eventSplitA, eventSplitB);
            JavaPairRDD<Tuple2<String, String>, List<Event>> keyValSplit = combinedEvents.mapToPair(keyValueFunction);
            JavaPairRDD<Tuple2<String, String>, List<Event>> grouped = keyValSplit.reduceByKey(combineFunction);
            JavaPairRDD<Tuple2<String, String>, List<Event>> filtered = grouped.filter(filterFunction);
            JavaPairRDD<Tuple2<String, String>, List<Event>> sorted = filtered.sortByKey(new TupleComparator(), true);
            JavaPairRDD<Tuple2<String, String>, List<Event>> partitioned = sorted.partitionBy(new CustomPartitioner(6));

            // Save the word count to a text file (initiates evaluation)
            partitioned.saveAsTextFile(outputFilename);
            
            // Print out accumulator values to System.out
            System.out.println("Number of Events Before Filtering: " + eventsBeforeFiltering.value());
            System.out.println("Number of Events After Filtering: " + eventsAfterFiltering.value());
            System.out.println("Number of Sessions: " + sessions.value());
            System.out.println("Number of SHOWER Sessions: " + showerSessions.value());
            System.out.println("Number of SHOWER Sessions Filtered Out: " + showerSessionsFiltered.value());
        } finally {
            // Shut down the context
            sc.stop();
        }
	}
	
	/*
	 * Takes a list as input and removes duplicates and then sorts based on
	 * timestamp (string), then type (string), then subtype (string)
	 */
	private static List<Event> removeDuplicatesAndSort(List<Event> events){
		List<Event> result = Lists.newArrayList();
		
		// Sort by chapter and verse
		for(Event e : events) {
			if(result.isEmpty())
				result.add(e);
			else {
				boolean added = false;
				String currentTimestamp = e.eventTimestamp;
				String currentType = e.eventType;
				String currentSubType = e.eventSubType;
				String currentVin = e.vin;
				
				for(int i = 0; i < result.size(); i++) {
					Event other = result.get(i);
					String otherTimestamp = other.eventTimestamp;
					String otherType = other.eventType;
					String otherSubType = other.eventSubType;
					String otherVin = other.vin;
					
					// compare timestamp
					if(currentTimestamp.compareTo(otherTimestamp) == 0) {
						// compare type
						if(currentType.compareTo(otherType) == 0) {
							// compare subtype
							if(currentSubType.compareTo(otherSubType) == 0) {
								// compare vin
								if(currentVin.compareTo(otherVin) == 0) {
									// Remove duplicate events
									added = true;
									break;
								} else {
									if(currentVin.compareTo(otherVin) < 0) {
										if(i == 0)
											result.add(0, e);
										else
											result.add(i, e);
										added = true;
										break;
									} 
								}
							} else {
								if(currentSubType.compareTo(otherSubType) < 0) {
									if(i == 0)
										result.add(0, e);
									else
										result.add(i, e);
									added = true;
									break;
								} 
							}
						} else {
							if(currentType.compareTo(otherType) < 0) {
								if(i == 0)
									result.add(0, e);
								else
									result.add(i, e);
								added = true;
								break;
							} 
						}
					} else {
						if(currentTimestamp.compareTo(otherTimestamp) < 0) {
							if(i == 0)
								result.add(0, e);
							else
								result.add(i, e);
							added = true;
							break;
						} 
					}
				}
				
				// Add to end if not added 
				if(!added) {
					result.add(e);
				}
			}
		}
		return result;
	}
	
	/* 
	 * Event data structure class 
	 */
	private static class Event implements Serializable { 
		String eventType;
		String eventSubType;
		String eventTimestamp;
		String vin; 
		
		public Event () {}
		
		public String toString() { 
			return "<" + eventType + ":" + eventSubType + "," + eventTimestamp + ">";
		} 
	}
	
	/*
	 * Custom Comparator class definition
	 */
	private static class TupleComparator implements Comparator<Tuple2<String, String>>, Serializable {
		@Override
		public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
			String o1UserId = o1._1;
			String o2UserId = o2._1;
			if(o1UserId.equals(o2UserId)) {
				String o1City = o1._2;
				String o2City = o2._2;
				return o1City.compareTo(o2City);
			}
			return o1UserId.compareTo(o2UserId);
		}
		
	}

}
