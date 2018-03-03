package com.refactorlabs.cs378.assign12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Spark Datasets application for Spark.
 */
public class SparkDatasets {
	
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(SparkDatasets.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();

		try {
            // Load the input data
            Dataset<Row> data = session.read().csv(inputFilename);

            // Filter out header and where price = 0
            FilterFunction<Row> filterFunction =
                    new FilterFunction<Row>() {
                        @Override
                        public boolean call(Row row) throws Exception {
                        		if(row.getAs("price").equals("price"))
                        			return false;
                        		Double price = new Double(row.getAs("price"));
                        		if(price.equals(0.0))
                        			return false;
                        		return true;
                        }
                    };
                    
            // Filter out header and where mileage = 0
            FilterFunction<Row> mileageFilterFunction =
                    new FilterFunction<Row>() {
                        @Override
                        public boolean call(Row row) throws Exception {
                        		if(row.getAs("mileage").equals("mileage"))
                        			return false;
                        		Double price = new Double(row.getAs("mileage"));
                        		if(price.equals(0.0))
                        			return false;
                        		return true;
                        }
                    };
                    
                    
            Dataset<Row> df = data.toDF("userID", "event", "timestamp", "vin", "condition", "year", "make", "model", "price", "mileage"); 
            
            // Make and model min, max, and average price
            Dataset<Row> filtered = df.filter(filterFunction).dropDuplicates("vin");
            filtered.createOrReplaceTempView("makeModel");
            Dataset<Row> makeModel = session.sql("SELECT make, model, MIN(CAST(price AS DECIMAL)), MAX(CAST(price AS DECIMAL)), AVG(CAST(price AS DECIMAL)) FROM makeModel GROUP BY make, model ORDER BY make, model");
            
            // Year min, max, and average mileage
            Dataset<Row> filteredMileage = df.filter(mileageFilterFunction).dropDuplicates("vin");
            filteredMileage.createOrReplaceTempView("yearMileage");
            Dataset<Row> yearMileage = session.sql("SELECT year, MIN(CAST(mileage AS DECIMAL)), MAX(CAST(mileage AS DECIMAL)), AVG(CAST(mileage AS DECIMAL)) FROM yearMileage GROUP BY year ORDER BY year");
            
            // Vin count for event type
            Dataset<Row> eventSplitDF = df.withColumn("event_type", functions.substring_index(df.col("event"), " ", 1));
            eventSplitDF.createOrReplaceTempView("vinEventCount");
            Dataset<Row> vinEventCount = session.sql("SELECT vin, event_type AS event, COUNT(*) FROM vinEventCount GROUP BY vin, event_type ORDER BY vin, event_type");
            
            // Save the query results to a text file (initiates evaluation)
            makeModel.repartition(1).write().format("csv").save(outputFilename + "/make_model");
            yearMileage.repartition(1).write().format("csv").save(outputFilename + "/year");
            vinEventCount.repartition(1).write().format("csv").save(outputFilename + "/vin");
            
        } finally {
            // Shut down the context
            sc.stop();
        }
	}

}
