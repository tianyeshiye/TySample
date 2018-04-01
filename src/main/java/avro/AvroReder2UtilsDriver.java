package avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class AvroReder2UtilsDriver {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("AvroWriterDriver").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, canInfo> records = AvroUtils.<canInfo>loadAvroFile(sc, "data/arvo-out/arvo1522505109643");

        records.foreach(new VoidFunction<Tuple2<String, canInfo>>() {
			@Override
			public void call(Tuple2<String, canInfo> tp10) throws Exception {
				System.out.println( tp10._1+ " "+ tp10._2.get("canId") + " " + String.valueOf(tp10._2.get("canTime")));
			}
		});

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
