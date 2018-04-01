package sample.spark.streaming.spark.avro;


import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class AvroRederDriver {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("AvroWriterDriver").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

     // Creates a DataFrame from a file
     Dataset df = sqlContext.read().format("com.databricks.spark.avro").load("data/arvo-out/arvo1522471539685");
     df.show();
    }

    private static Job getJob() {
    	Job job;
    	try {
    		job = Job.getInstance();
    	} catch(Exception e) {
    		e.printStackTrace();
    		throw new RuntimeException(e);
    	}
    	return job;
    }
}
