package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Example MapReduce program that examines user events and combines them into one
 * session using Avro.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Base word count program by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class UserSessions extends Configured implements Tool {

	/**
	 * The Map class for user sessions.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for user sessions.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, 
											   Text, AvroValue<Session>> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();
		
		// For reference only
		private String[] fieldNames = {"user_id", "event_type", "event_timestamp", "city", 
									  "vin", "vehicle_condition", "year", "make", "model", 
									  "trim", "body_style", "cab_style", "price", "mileage", 
									  "free_carfax_report", "features"};

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitLine = line.split("\t");

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			word = new Text(splitLine[0]);
			Session.Builder sessionBuilder = generateSessionBuilder(splitLine);
			context.write(word, new AvroValue<Session>(sessionBuilder.build()));
			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
		}
		
		/*
		 * Builds the builder for the Session object
		 */
		private Session.Builder generateSessionBuilder(String[] splitLine){
			Session.Builder sessionBuilder = Session.newBuilder();
			
			// Build Event Builder
			Event.Builder eventBuilder = generateEventBuilder(splitLine);
			ArrayList<Event> eventList = new ArrayList<Event>();
			eventList.add(eventBuilder.build());
			
			sessionBuilder.setEvents(eventList);
			sessionBuilder.setUserId(splitLine[0]);
			
			return sessionBuilder;
		}
		
		/*
		 * Builds the builder for the Event object
		 */
		private Event.Builder generateEventBuilder(String[] splitLine){
			Event.Builder userEvent = Event.newBuilder();
			String[] eventTypeSplit = splitLine[1].split(" ");
			
			// Set Event Type
			for (EventType et : EventType.values()) {
		        if (et.name().equals(eventTypeSplit[0].toUpperCase())) {
		        		userEvent.setEventType(et);
		        }
		    }
			
			// Set Event Subtype
			String eventSub = eventTypeSplit[1];
			if(eventTypeSplit.length > 2) {
				eventSub += "_" + eventTypeSplit[2];
			} 
			for (EventSubtype est : EventSubtype.values()) {
				if(est.name().equals(eventSub.toUpperCase())) {
					userEvent.setEventSubtype(est);
				}
			}
			
			// Set Time, City, VIN
			userEvent.setEventTime(splitLine[2]);
			userEvent.setCity(splitLine[3]);
			userEvent.setVin(splitLine[4]);
			
			// Set Condition
			for (Condition c : Condition.values()) {
				if(c.name().equals(splitLine[5])) {
					userEvent.setCondition(c);
				}
			}
			
			// Set Year, Make, Model, Trim
			userEvent.setYear(splitLine[6]);
			userEvent.setMake(splitLine[7]);
			userEvent.setModel(splitLine[8]);
			userEvent.setTrim(splitLine[9]);
			
			// Set Body Style
			for (BodyStyle bs : BodyStyle.values()) {
				if(bs.name().equals(splitLine[10])) {
					userEvent.setBodyStyle(bs);
				}
			}
			
			// Set Cab Style
			userEvent.setCabStyle(null);
			String cabString = splitLine[11];
			String[] cabSplit = cabString.split(" ");
			for (CabStyle cs : CabStyle.values()) {
				if(cs.name().equals(cabSplit[0])) {
					userEvent.setCabStyle(cs);
				} 
			}
			
			// Set Price, Mileage, Car Fax Report
			Double price = new Double(splitLine[12]);
			userEvent.setPrice(price.doubleValue());
			Long mileage = new Long(splitLine[13]);
			userEvent.setMileage(mileage.longValue());
			if(splitLine[14].equals("f")) {
				userEvent.setFreeCarfaxReport(false);
			} else {
				userEvent.setFreeCarfaxReport(true);
			}
			
			// Set Features
			List<CharSequence> features = new ArrayList<CharSequence>();
			String featureLine = splitLine[15];
			String[] featureSplit = featureLine.split(":");
			Arrays.sort(featureSplit);
			for(int i = 0; i < featureSplit.length; i++) {
				features.add(featureSplit[i]);
			}
			userEvent.setFeatures(features);
			
			return userEvent;
		}
		
	}

	/**
	 * The Reduce class for user sessions.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for user sessions.
	 */
	public static class CombinerClass extends Reducer<Text, AvroValue<Session>, 
												     Text, AvroValue<Session>> {

		public void combine(Text key, Iterable<AvroValue<Session>> values, Context context)
				throws IOException, InterruptedException {
			Session.Builder sessionBuilder = Session.newBuilder();
			List<Event> eventList = new ArrayList<Event>();
			String returnKey = "";

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Construct a master list
			for (AvroValue<Session> value : values) {
				returnKey = value.datum().getUserId().toString();
				List<Event> tempList = value.datum().getEvents();
				for(Event e : tempList) {
					eventList.add(e);
				}
			}
			
			// Sort the list and eliminate duplicates
			List<Event> sortedList = sortArray(eventList);
			List<Integer> indexes = new ArrayList<Integer>();
			for(int i = 1; i < sortedList.size(); i++) {
				if(checkDuplicate(sortedList.get(i - 1), sortedList.get(i))) {
					indexes.add(0, new Integer(i));
				}
			}
			for(int i = 0; i < indexes.size(); i++) {
				sortedList.remove(indexes.get(i).intValue());
			}
			
			sessionBuilder.setEvents(sortedList);
			sessionBuilder.setUserId(key.toString());
			
			context.write(new Text(returnKey), new AvroValue<Session>(sessionBuilder.build()));
		}
		
		/*
		 * Checks the Event object for duplicates by looking at every field
		 */
		private boolean checkDuplicate(Event e1, Event e2) {
			boolean result = true;
			
			for(int i = 0; i < 16; i++) {
				if(e1.get(i) == null && e2.get(i) == null) {
					continue;
				} else if(e1.get(i) == null || e2.get(i) == null) {
					result = false;
					break;
				} else {
					if(!e1.get(i).equals(e2.get(i))) {
						result = false;
						break;
					}
				}
			}
			return result;
		}
		
		/*
		 * Sorts the array based on Event Time and if that is 
		 * identical then it is based on Event Type
		 */
		private List<Event> sortArray(List<Event> unsortedList){
			List<Event> sortedList = new ArrayList<Event>();
			for(Event e : unsortedList) {
				sortedList.add(e);
				int cmp = 0;
				int currentIndex = sortedList.size() - 1;
				for(int i = sortedList.size() - 1; i > 0; i--) {
					cmp = compare(e, sortedList.get(i - 1));
					if(cmp > 0) {
						break;
					} else if (cmp < 0) {
						sortedList.remove(sortedList.get(currentIndex));
						sortedList.add(i - 1, e);
						currentIndex = i - 1;
					} 
				}
			}
			return sortedList;
		}
		
		/*
		 * Compares two event types to one another based on Event Time
		 * and Event Type as backup
		 */
		private int compare(Event e1, Event e2) {
			int cmp = 0;
			String e1Time = e1.getEventTime().toString();
			EventType e1Type = e1.getEventType();
			String e2Time = e2.getEventTime().toString();
			EventType e2Type = e2.getEventType();
			
			cmp = e1Time.compareTo(e2Time);
			if(cmp == 0) {
				cmp = e1Type.ordinal() - e2Type.ordinal();
			}
			return cmp;
		}

	}
	
	/**
	 * The Reduce class for user sessions.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for user sessions.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<Session>, 
												   AvroKey<CharSequence>, AvroValue<Session>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
				throws IOException, InterruptedException {
			Session.Builder sessionBuilder = Session.newBuilder();
			List<Event> eventList = new ArrayList<Event>();
			AvroKey<CharSequence> returnKey = null;

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Construct a master list
			for (AvroValue<Session> value : values) {
				returnKey = new AvroKey<CharSequence>(value.datum().getUserId());
				List<Event> tempList = value.datum().getEvents();
				for(Event e : tempList) {
					eventList.add(e);
				}
			}
			
			// Sort the list and eliminate duplicates
			List<Event> sortedList = sortArray(eventList);
			List<Integer> indexes = new ArrayList<Integer>();
			for(int i = 1; i < sortedList.size(); i++) {
				if(checkDuplicate(sortedList.get(i - 1), sortedList.get(i))) {
					indexes.add(0, new Integer(i));
				}
			}
			for(int i = 0; i < indexes.size(); i++) {
				sortedList.remove(indexes.get(i).intValue());
			}
			
			sessionBuilder.setEvents(sortedList);
			sessionBuilder.setUserId(key.toString());
			
			context.write(returnKey, new AvroValue<Session>(sessionBuilder.build()));
		}
		
		/*
		 * Checks the Event object for duplicates by looking at every field
		 */
		private boolean checkDuplicate(Event e1, Event e2) {
			boolean result = true;
			
			for(int i = 0; i < 16; i++) {
				if(e1.get(i) == null && e2.get(i) == null) {
					continue;
				} else if(e1.get(i) == null || e2.get(i) == null) {
					result = false;
					break;
				} else {
					if(!e1.get(i).equals(e2.get(i))) {
						result = false;
						break;
					}
				}
			}
			return result;
		}
		
		/*
		 * Sorts the array based on Event Time and if that is 
		 * identical then it is based on Event Type
		 */
		private List<Event> sortArray(List<Event> unsortedList){
			List<Event> sortedList = new ArrayList<Event>();
			for(Event e : unsortedList) {
				sortedList.add(e);
				int cmp = 0;
				int currentIndex = sortedList.size() - 1;
				for(int i = sortedList.size() - 1; i > 0; i--) {
					cmp = compare(e, sortedList.get(i - 1));
					if(cmp > 0) {
						break;
					} else if (cmp < 0) {
						sortedList.remove(sortedList.get(currentIndex));
						sortedList.add(i - 1, e);
						currentIndex = i - 1;
					} 
				}
			}
			return sortedList;
		}
		
		/*
		 * Compares two event types to one another based on Event Time
		 * and Event Type as backup
		 */
		private int compare(Event e1, Event e2) {
			int cmp = 0;
			String e1Time = e1.getEventTime().toString();
			EventType e1Type = e1.getEventType();
			String e2Time = e2.getEventTime().toString();
			EventType e2Type = e2.getEventType();
			
			cmp = e1Time.compareTo(e2Time);
			if(cmp == 0) {
				cmp = e1Type.ordinal() - e2Type.ordinal();
			}
			return cmp;
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

		Job job = Job.getInstance(conf, "UserSessions");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(UserSessions.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Session.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(CombinerClass.class);

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
		int res = ToolRunner.run(new UserSessions(), args);
		System.exit(res);
	}

}
