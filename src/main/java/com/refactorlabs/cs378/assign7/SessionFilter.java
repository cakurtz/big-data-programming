package com.refactorlabs.cs378.assign7;

import com.refactorlabs.cs378.sessions.*;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Example MapReduce program that examines two different files and performs
 * a reduce-side join utilizing left outer join.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Base word count program structure by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class SessionFilter extends Configured implements Tool {

	/**
	 * The Map class for session filtering.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the session filtering.
	 * The input to the mapper is user sessions. 
	 */
	public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, 
											   AvroKey<CharSequence>, AvroValue<Session>> {
		
		private AvroMultipleOutputs multipleOutputs; // For writing out to multiple files
		private Random rand = new Random(); // For random number generation in random sampling
		private Double clickerPercentage;  // Percentage of CLICKER events to include
		private Double showerPercentage; // Percentage of SHOWER events to include
		
		public void setup(Context context) {
		       multipleOutputs = new AvroMultipleOutputs(context);
		       
		       // Set up percentages
		       String clickerPercent = context.getConfiguration().get("clicker_filter_percentage");
		       clickerPercentage = Double.parseDouble(clickerPercent) / 100.0;
		       String showerPercent = context.getConfiguration().get("shower_filter_percentage");
		       showerPercentage = Double.parseDouble(showerPercent) / 100.0;
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
				
				// Clicker Random Sampling
				if(type.equals(SessionType.CLICKER)) {
					if(rand.nextDouble() < clickerPercentage)
						multipleOutputs.write(type.getText(), key, value);
					else
						context.getCounter(Utils.FILTER_COUNTER_GROUP, "clicker discards").increment(1L);
				}
				
				// Shower Random Sampling
				else if(type.equals(SessionType.SHOWER)) {
					if(rand.nextDouble() < showerPercentage)
						multipleOutputs.write(type.getText(), key, value);
					else
						context.getCounter(Utils.FILTER_COUNTER_GROUP, "shower discards").increment(1L);
				}
				
				// All other types
				else
					multipleOutputs.write(type.getText(), key, value);
			}
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
		
		// Set up filtering percentages
		conf.set("clicker_filter_percentage", "10");
		conf.set("shower_filter_percentage", "2");

		Job job = Job.getInstance(conf, "SessionFilter");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionFilter.class);

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

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new SessionFilter(), args);
		System.exit(res);
	}

}
