package avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class AvroRederDriver {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("AvroWriterDriver").setMaster("local[2]");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "function2.MyKryoRegistrator");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

     // Creates a DataFrame from a file
     Dataset df = sqlContext.read().format("com.databricks.spark.avro").load("data/arvo-out/arvo1522450431164");
     df.select("canId").show();

    }
    private static Job getJob(Schema avroSchema) {

        Job job;

        try {
            job = Job.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AvroJob.setInputKeySchema(job, avroSchema);

        return job;
    }
}
