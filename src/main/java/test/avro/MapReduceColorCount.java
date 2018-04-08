package test.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import avro.canInfo;

public class MapReduceColorCount extends Configured implements Tool {

    ///Mapper定义：
    ///输入Key类型是AvroKey<User>，输入Value类型是NullWritable
    ///输出Key类型是Text，输出Value类型是IntWritable
    public static class ColorCountMapper extends
            Mapper<AvroKey<canInfo>, NullWritable, Text, IntWritable> {

        @Override
        public void map(AvroKey<canInfo> key, NullWritable value, Context context)
                throws IOException, InterruptedException {

            CharSequence color = key.datum().getCanId();
            if (color == null) {
                color = "none";
            }
            context.write(new Text(color.toString()), new IntWritable(1));
        }
    }

    ///Reducer定义：
    ///输入Key类型是Text，输入Value类型是IntWritable(跟Key的输出Key/Value类型一致)
    ///输出Key类型是AvroKey<CharSequence>，输出Value类型是AvroValue<Integer>
    public static class ColorCountReducer extends
            Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(sum));
        }
    }

    public int run(String[] args) throws Exception {

        Job job = new Job(getConf());
        job.setJarByClass(MapReduceColorCount.class);
        job.setJobName("Color Count");

        ///指定输入路径，输入文件是Avro格式
        FileInputFormat.setInputPaths(job, new Path("input/avro"));

        ///指定输出路径，输出文件格式是Key/Value组成的Avro文件，见AvroKeyValueOutputFormat
        FileOutputFormat.setOutputPath(job, new Path("output" +   String.valueOf(System.currentTimeMillis())));

        //AvroKeyInputFormat: A MapReduce InputFormat that can handle Avro container files.
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(ColorCountMapper.class);
        AvroJob.setInputKeySchema(job, canInfo.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ColorCountReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MapReduceColorCount(), args);
        System.exit(res);
    }
}