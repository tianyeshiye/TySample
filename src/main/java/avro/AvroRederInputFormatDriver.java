package avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class AvroRederInputFormatDriver {

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("AvroWriterDriver").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		AvroSchemaMapper mapper = new AvroSchemaMapper();
		Job job = getJob(mapper.getSchema());

		//@SuppressWarnings("unchecked")
		JavaPairRDD<AvroKey, NullWritable> inputRecords = (JavaPairRDD<AvroKey, NullWritable>) sc.newAPIHadoopFile(
				"data/arvo-out/arvo1522450431164", AvroKeyInputFormat.class,
				GenericRecord.class, NullWritable.class, job.getConfiguration());
		inputRecords.foreach(new VoidFunction<Tuple2<AvroKey, NullWritable>>() {
			@Override
			public void call(Tuple2<AvroKey, NullWritable> tp10) throws Exception {
				GenericRecord record = (GenericRecord) tp10._1.datum();
				List<Field> fields = record.getSchema().getFields();
				for(Field field :fields) {
					System.out.println(field.name() + ":" + record.get(field.name()));
				}
				System.out.println(record.get("canId") + " " + String.valueOf(record.get("canTime")));
			}
		});

//		for (field <- record.getSchema.getFields) {
//			  // 方法1
//			  println(field.name, record.get(field.name))
//			  // 方法2
//			  println(field.name, record.get(field.pos))
//			}
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
