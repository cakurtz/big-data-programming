package com.refactorlabs.cs378.assign8;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.assign7.SessionType;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Example MapReduce program that examines a file of user sessions
 * and filters them into bins. These bins are used as input for
 * parallel jobs, one for each bin, that computes statistics of the
 * event subtype for the different session types. The final job
 * combines all the statistics into one file.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Base word count program structure by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class SessionStatistics extends Configured implements Tool {

	/**
	 * The Map class for session filtering.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the session filtering.
	 * The input to the mapper is user sessions. 
	 */
	public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, 
											   AvroKey<CharSequence>, AvroValue<Session>> {
		
		private AvroMultipleOutputs multipleOutputs; // For writing out to multiple files
		
		public void setup(Context context) {
		       multipleOutputs = new AvroMultipleOutputs(context);
		}
		
		public void cleanup(Context context)
		          throws InterruptedException, IOException{
		       multipleOutputs.close();
		}
		
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			List<Event> sessionEvents = value.datum().getEvents();
			SessionType type = SessionType.OTHER;

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			// If session has more than 100 events, filter out
			if(sessionEvents.size() > 100) {
				context.getCounter(Utils.FILTER_COUNTER_GROUP, "large sessions discarded").increment(1L);
			}
			else {
				// Look at each event in session and determine session type
				for(Event e : sessionEvents) {
					EventType cur = e.getEventType();
					if(cur.equals(EventType.CHANGE) || cur.equals(EventType.EDIT) || cur.equals(EventType.SUBMIT)) {
						type = SessionType.SUBMITTER;
						break;
					}
					else if(cur.equals(EventType.CLICK))
						type = SessionType.CLICKER;
					else if((cur.equals(EventType.SHOW) || cur.equals(EventType.DISPLAY))
							&& !type.equals(SessionType.CLICKER))
						type = SessionType.SHOWER;
					else if(cur.equals(EventType.VISIT) && !type.equals(SessionType.CLICKER) 
							&& !type.equals(SessionType.SHOWER)) 
						type = SessionType.VISITOR;
				}
				
				// Write out session
				context.getCounter(Utils.FILTER_COUNTER_GROUP, type.getText()).increment(1L);
				multipleOutputs.write(type.getText(), key, value);
			}
		}
	}
	
	/**
	 * The Map class for session statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the session statistics.
	 * The input to the mapper is user sessions. 
	 */
	public static class StatisticsMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, 
											   			 AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
		
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			String[] subtypes = { "ALTERNATIVE", "BADGE_DETAIL", 
								 "BADGES", "CONTACT_BANNER", 
								 "CONTACT_BUTTON", "CONTACT_FORM",  
								 "DEALER_PHONE", "FEATURES", 
								 "GET_DIRECTIONS", "MARKET_REPORT",
								 "PHOTO_MODAL", "VEHICLE_HISTORY" };
			List<Event> sessionEvents = value.datum().getEvents();
			Map<String, EventSubtypeStatisticsData.Builder> resultMap = new HashMap<String, EventSubtypeStatisticsData.Builder>();
			Map<String, EventSubtypeStatisticsData.Builder> anyMap = new HashMap<String, EventSubtypeStatisticsData.Builder>();
			SessionType type = SessionType.OTHER;
			Boolean sessionCountSet = false;
			Long sessionCountValue = 1L;
			
			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines to StatisticsMapClass").increment(1L);
			
			// Initialize map to hold session results
			for(String s : subtypes) {
				EventSubtypeStatisticsData.Builder builder = EventSubtypeStatisticsData.newBuilder();
				EventSubtypeStatisticsData.Builder anyBuilder = EventSubtypeStatisticsData.newBuilder();
				resultMap.put(s, builder);
				anyMap.put(s, anyBuilder);
			}
			
			// Look at each event in session and determine session type
			for(Event e : sessionEvents) {
				EventType cur = e.getEventType();
				if(cur.equals(EventType.CHANGE) || cur.equals(EventType.EDIT) || cur.equals(EventType.SUBMIT)) {
					type = SessionType.SUBMITTER;
					break;
				}
				else if(cur.equals(EventType.CLICK))
					type = SessionType.CLICKER;
				else if((cur.equals(EventType.SHOW) || cur.equals(EventType.DISPLAY))
						&& !type.equals(SessionType.CLICKER))
					type = SessionType.SHOWER;
				else if(cur.equals(EventType.VISIT) && !type.equals(SessionType.CLICKER) 
						&& !type.equals(SessionType.SHOWER)) 
					type = SessionType.VISITOR;
			}
			
			// Insert statistics into result map
			for(Event e : sessionEvents) {
				String eventSubtype = e.getEventSubtype().toString();
				EventSubtypeStatisticsData.Builder builder = resultMap.get(eventSubtype);
				EventSubtypeStatisticsData.Builder anyBuilder = anyMap.get(eventSubtype);
				builder.setTotalCount(builder.getTotalCount() + 1);
				anyBuilder.setTotalCount(anyBuilder.getTotalCount() + 1);
				if(!sessionCountSet) {
					builder.setSessionCount(sessionCountValue);
					anyBuilder.setSessionCount(sessionCountValue);
					sessionCountSet = true;
				}
				resultMap.put(eventSubtype, builder);
				anyMap.put("ANY", anyBuilder);
			}
			
			// Session Type
			for(String s : resultMap.keySet()) {
				EventSubtypeStatisticsKey.Builder resultKey = EventSubtypeStatisticsKey.newBuilder();
				resultKey.setSessionType(type.getText().toUpperCase());
				resultKey.setEventSubtype(s);
				
				EventSubtypeStatisticsData.Builder builder = resultMap.get(s);
				Long square = builder.getTotalCount();
				builder.setSumOfSquares(square*square);
				
				context.write(new AvroKey<EventSubtypeStatisticsKey>(resultKey.build()), new AvroValue<EventSubtypeStatisticsData>(resultMap.get(s).build()));
			}
			
			// ANY Session Type
			for(String s : resultMap.keySet()) {
				EventSubtypeStatisticsKey.Builder resultKey = EventSubtypeStatisticsKey.newBuilder();
				resultKey.setSessionType("ANY");
				resultKey.setEventSubtype(s);
				
				EventSubtypeStatisticsData.Builder builder = anyMap.get(s);
				Long square = builder.getTotalCount();
				builder.setSumOfSquares(square*square);
				
				context.write(new AvroKey<EventSubtypeStatisticsKey>(resultKey.build()), new AvroValue<EventSubtypeStatisticsData>(resultMap.get(s).build()));
			}
		}
	}
	
	/**
	 * The Map class for session statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the session statistics.
	 * The input to the mapper is EventSubtypeStatisticsKey and EventSubtypeStatisticsData. 
	 */
	public static class CombineMapClass extends Mapper<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>, 
											   		  AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
		
		@Override
		public void map(AvroKey<EventSubtypeStatisticsKey> key, AvroValue<EventSubtypeStatisticsData> value, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines to CombineMapClass").increment(1L);
			
			context.write(key, value);
		}
	}
	
	/**
	 * The Map class for session statistics.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the session statistics.
	 * The input to the reducer is EventSubtypeStatisticsKey and EventSubtypeStatisticsData. 
	 */
	public static class CombineReduceClass extends Reducer<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>, 
											   		      AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
		
		@Override
		public void reduce(AvroKey<EventSubtypeStatisticsKey> key, Iterable<AvroValue<EventSubtypeStatisticsData>> values, Context context)
				throws IOException, InterruptedException {

			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Input Lines to CombineMapClass").increment(1L);
			
			EventSubtypeStatisticsData.Builder totalStats = EventSubtypeStatisticsData.newBuilder();
			
			// Set total count and set of squares
			for(AvroValue<EventSubtypeStatisticsData> v : values) {
				// Set total count
				Long totalCount = totalStats.getTotalCount();
				totalCount += v.datum().getTotalCount();
				totalStats.setTotalCount(totalCount);
				
				// Set session count
				Long sessionCount = totalStats.getSessionCount();
				sessionCount += v.datum().getSessionCount();
				totalStats.setSessionCount(sessionCount);
				
				// Set sum of squares
				Long sumOfSquares = totalStats.getSumOfSquares();
				sumOfSquares += v.datum().getSumOfSquares();
				totalStats.setSumOfSquares(sumOfSquares);
			}
			
			// Compute mean
			double mean = 0.0;
			mean = ((double) totalStats.getTotalCount()) / ((double) totalStats.getSessionCount());
			totalStats.setMean(mean);
			
			// Compute variance
			double variance = 0.0;
			if((totalStats.getSessionCount() - (mean * mean)) > 0)
				variance = (totalStats.getSumOfSquares().doubleValue() / totalStats.getSessionCount().doubleValue()) - (mean * mean);
			totalStats.setVariance(variance);
			
			context.write(key, new AvroValue<EventSubtypeStatisticsData>(totalStats.build()));
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

		Job job = Job.getInstance(conf, "SessionStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Session.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Session.class);
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);

		// Set the input and output file formats.
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));
		
		// Set up multiple outputs
		AvroMultipleOutputs.setCountersEnabled(job, true);
		AvroMultipleOutputs.addNamedOutput(job, "submitter", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "clicker", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "shower", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "visitor", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "other", AvroKeyValueOutputFormat.class, 
				Schema.create(Schema.Type.STRING), Session.getClassSchema());

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		// Initiate a job for each file output and wait for completion 
		Job submitterJob = createJob(conf, "SubmitterStatistics", appArgs[1] + "/submitter-m-00000.avro", appArgs[1] + "/submitter");
		Job clickerJob = createJob(conf, "ClickerStatistics", appArgs[1] + "/clicker-m-00000.avro", appArgs[1] + "/clicker");
		Job showerJob = createJob(conf, "ShowerStatistics", appArgs[1] + "/shower-m-00000.avro", appArgs[1] + "/shower");
		Job visitorJob = createJob(conf, "VisitorStatistics", appArgs[1] + "/visitor-m-00000.avro", appArgs[1] + "/visitor");
		
		while(!submitterJob.isComplete() || !clickerJob.isComplete() || !showerJob.isComplete() || !visitorJob.isComplete()) {
			Thread.sleep(5000);
		}
		
		// Set up final job
		Job finalJob = Job.getInstance(conf, "ResultStatistics");
		finalJob.setJarByClass(SessionStatistics.class);
		
		// Set output Types
		finalJob.setOutputKeyClass(EventSubtypeStatisticsKey.class);
		finalJob.setOutputValueClass(EventSubtypeStatisticsData.class);
		finalJob.setMapOutputKeyClass(EventSubtypeStatisticsKey.class);
		finalJob.setMapOutputValueClass(EventSubtypeStatisticsData.class);
		AvroJob.setOutputValueSchema(finalJob, EventSubtypeStatisticsData.getClassSchema());
		AvroJob.setOutputKeySchema(finalJob, EventSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputKeySchema(finalJob, EventSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(finalJob, EventSubtypeStatisticsData.getClassSchema());
		
		// Set mapper and reducer
		finalJob.setMapperClass(CombineMapClass.class);
		finalJob.setReducerClass(CombineReduceClass.class);
		
		// Set Input and Format
		finalJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		AvroJob.setInputKeySchema(finalJob, EventSubtypeStatisticsKey.getClassSchema());
		AvroJob.setInputValueSchema(finalJob, EventSubtypeStatisticsData.getClassSchema());
		finalJob.setOutputFormatClass(TextOutputFormat.class);
		
		// Set Input/Output Paths
		FileInputFormat.addInputPath(finalJob, new Path(appArgs[1] + "/submitter/part-m-00000.avro"));
		FileInputFormat.addInputPath(finalJob, new Path(appArgs[1] + "/clicker/part-m-00000.avro"));
		FileInputFormat.addInputPath(finalJob, new Path(appArgs[1] + "/shower/part-m-00000.avro"));
		FileInputFormat.addInputPath(finalJob, new Path(appArgs[1] + "/visitor/part-m-00000.avro"));
		FileOutputFormat.setOutputPath(finalJob, new Path(appArgs[1] + "/results"));
		
		finalJob.waitForCompletion(true);
		
		return 0;
	}
	
	/*
	 * Builds the job for a specific bin
	 */
	public Job createJob(Configuration conf, String jobName, String inputPath, String outputPath) throws Exception {
		Job newJob = Job.getInstance(conf, jobName);
		newJob.setJarByClass(SessionStatistics.class);
		
		// Set output Types
		newJob.setOutputKeyClass(EventSubtypeStatisticsKey.class);
		newJob.setOutputValueClass(EventSubtypeStatisticsData.class);
		newJob.setMapOutputKeyClass(EventSubtypeStatisticsKey.class);
		newJob.setMapOutputValueClass(EventSubtypeStatisticsData.class);
		AvroJob.setOutputValueSchema(newJob, EventSubtypeStatisticsData.getClassSchema());
		AvroJob.setOutputKeySchema(newJob, EventSubtypeStatisticsKey.getClassSchema());
		
		// Set mapper and reducer
		newJob.setMapperClass(StatisticsMapClass.class);
		newJob.setNumReduceTasks(0);
		
		// Set Input and Format
		newJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		AvroJob.setInputKeySchema(newJob, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(newJob, Session.getClassSchema());
		newJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		// Set Input/Output Paths
		FileInputFormat.addInputPath(newJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(newJob, new Path(outputPath));
		
		newJob.submit();
		return newJob;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new SessionStatistics(), args);
		System.exit(res);
	}

}
