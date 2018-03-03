package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
 * Example MapReduce program that examines two different files and performs
 * a reduce-side join utilizing left outer join.
 *
 * @author Coty Kurtz (cakurtz@utexas.edu)
 * 
 * Base word count program structure by
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class ReduceSideJoin extends Configured implements Tool {

	/**
	 * The Map class for reduce-side join.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the "left" side of left outer join.
	 * The input to the mapper is user sessions. 
	 */
	public static class SessionMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, 
											   Text, AvroValue<VinImpressionCounts>> {
		
		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
			List<Event> sessionEvents = value.datum().getEvents();
			HashMap<CharSequence, VinImpressionCounts.Builder> vins = new HashMap<CharSequence, VinImpressionCounts.Builder>();

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			for(Event e : sessionEvents) {
				CharSequence currentVin = e.getVin();
				if(!vins.containsKey(currentVin)) {
					VinImpressionCounts.Builder vinBuilder = generateBuilder(e);
					vins.put(currentVin, vinBuilder);
				}
				else {
					VinImpressionCounts.Builder update = updateBuilder(vins.get(currentVin), e);
					vins.put(currentVin, update);
				}
			}
			
			for(CharSequence entry : vins.keySet()) {
				context.write(new Text(entry.toString()), new AvroValue<VinImpressionCounts>(vins.get(entry).build()));
				context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}
		}
		
		/*
		 * Updates the builder for the VinImpressionCounts object
		 */
		private VinImpressionCounts.Builder updateBuilder(VinImpressionCounts.Builder currentBuilder, Event event){
			VinImpressionCounts.Builder update = currentBuilder;
			
			// Update clicks if event type is CLICK
			if(event.getEventType().equals(EventType.CLICK)) {
				String subtype = event.getEventSubtype().toString();
				Map<CharSequence, Long> clicks = currentBuilder.getClicks();
				if(!clicks.containsKey(subtype)) {
					clicks.put(subtype, 1L);
					currentBuilder.setClicks(clicks);
				}
			}
			
			// Set editContactForm if type and subtype equal EDIT and CONTACT_FORM
			if(event.getEventType().equals(EventType.EDIT) && event.getEventSubtype().equals(EventSubtype.CONTACT_FORM)) {
				currentBuilder.setEditContactForm(1L);
			}
			
			return update;
		}
		
		/*
		 * Builds the builder for the VinImpressionCounts object
		 */
		private VinImpressionCounts.Builder generateBuilder(Event event){
			VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
			
			// Set unique users to 1
			vinBuilder.setUniqueUsers(1L);
			
			// Build map for clicks and insert if event type is CLICK
			HashMap<CharSequence, Long> clicks = new HashMap<CharSequence, Long>();
			if(event.getEventType().equals(EventType.CLICK)) {
				clicks.put(event.getEventSubtype().toString(), 1L);
			}
			vinBuilder.setClicks(clicks);
			
			// Set editContactForm if type and subtype equal EDIT and CONTACT_FORM
			if(event.getEventType().equals(EventType.EDIT) && event.getEventSubtype().equals(EventSubtype.CONTACT_FORM)) {
				vinBuilder.setEditContactForm(1L);
			}
			
			return vinBuilder;
		}
		
	}
	
	/**
	 * The second Map class for reduce-side join.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the "right" side of left outer join. 
	 * The input to this mapper is a CSV file with vin counts.
	 */
	public static class VinMapClass extends Mapper<LongWritable, Text, 
											   	  Text, AvroValue<VinImpressionCounts>> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splitLine = line.split(",");

			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
			
			String vin = splitLine[0];
			VinImpressionCounts.Builder vinBuilder = generateBuilder(splitLine);
			
			context.write(new Text(vin), new AvroValue<VinImpressionCounts>(vinBuilder.build()));
			context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
		}
		
		/*
		 * Builds the builder for the VinImpressionCounts object
		 */
		private VinImpressionCounts.Builder generateBuilder(String[] splitLine){
			VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
			
			if(splitLine[1].equals("SRP")) {
				vinBuilder.setMarketplaceSrps(Long.parseLong(splitLine[2]));
			} else {
				vinBuilder.setMarketplaceVdps(Long.parseLong(splitLine[2]));
			}
			
			return vinBuilder;
		}
		
	}
	
	/**
	 * The Reduce class for reduce-side join.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for reduce side-join.
	 * Only objects that appear in the "left" side are included and corresponding
	 * "right" side data is added. 
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>, 
												   Text, AvroValue<VinImpressionCounts>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
				throws IOException, InterruptedException {
			VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
			long unique_users = 0, edit_contact_form = 0, marketplace_srps = 0, marketplace_vdps = 0;
			HashMap<CharSequence, Long> clicks = new HashMap<CharSequence, Long>();
			
			context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			for(AvroValue<VinImpressionCounts> v : values) {
				// Right side of the join
				if(v.datum().getUniqueUsers().equals(0L)) {
					marketplace_srps += v.datum().getMarketplaceSrps();
					marketplace_vdps += v.datum().getMarketplaceVdps();
				}
				
				// Left side of the join
				else {
					unique_users += v.datum().getUniqueUsers();
					edit_contact_form += v.datum().getEditContactForm();
					Map<CharSequence, Long> temp = v.datum().getClicks();
					for(CharSequence c : temp.keySet()) {
						if(clicks.containsKey(c)) {
							Long count = clicks.get(c);
							count += temp.get(c);
							clicks.put(c, count);
						} else {
							clicks.put(c, temp.get(c));
						}
					}
				}
			}
			
			// Put all values in builder and build only if vin exists in left side of data
			if(unique_users != 0) {
				vinBuilder.setUniqueUsers(unique_users);
				vinBuilder.setEditContactForm(edit_contact_form);
				vinBuilder.setMarketplaceSrps(marketplace_srps);
				vinBuilder.setMarketplaceVdps(marketplace_vdps);
				vinBuilder.setClicks(clicks);
				context.write(key, new AvroValue<VinImpressionCounts>(vinBuilder.build()));
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

		Job job = Job.getInstance(conf, "ReduceSideJoin");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(ReduceSideJoin.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VinImpressionCounts.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VinImpressionCounts.class);
		AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

		// Set the map and reduce classes.
		job.setMapperClass(SessionMapClass.class);
		job.setReducerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionMapClass.class);
		MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, VinMapClass.class);
		MultipleInputs.addInputPath(job, new Path(appArgs[2]), TextInputFormat.class, VinMapClass.class);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[3]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new ReduceSideJoin(), args);
		System.exit(res);
	}

}
