package sample.spark.streaming.spark.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import jodd.util.StringUtil;
import sample.spark.streaming.CanUnitBean;
import sample.spark.streaming.TelegramHash;
import sample.spark.streaming.Unit;
import scala.Tuple2;

public class AvroWriterDriver {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("AvroWriterDriver").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Job job = getJob();
        AvroSchemaMapper mapper = new AvroSchemaMapper();
        AvroJob.setOutputKeySchema(job, mapper.getSchema());

        org.apache.hadoop.conf.Configuration confHaddop = job.getConfiguration();

        // 1506787111000 0x01 0x22 0x122 0x201
        TelegramHash telegramHash1 = new TelegramHash("1111", 1506787111000L);
        CanUnitBean canUnitBean11 = Unit.createCanUnitBean((short) 0x01, "1506787111010");
        CanUnitBean canUnitBean12 = Unit.createCanUnitBean((short) 0x22, "1506787111010");
        CanUnitBean canUnitBean13 = Unit.createCanUnitBean((short) 0x122, "1506787111030");
        CanUnitBean canUnitBean14 = Unit.createCanUnitBean((short) 0x201, "1506787111040");

        CanUnitBean canUnitBean11_1 = Unit.createCanUnitBean2((short) 0x01, "1506787111010");
        CanUnitBean canUnitBean12_1 = Unit.createCanUnitBean2((short) 0x22, "1506787111020");
        CanUnitBean canUnitBean13_1 = Unit.createCanUnitBean2((short) 0x122, "1506787111030");
        CanUnitBean canUnitBean14_1 = Unit.createCanUnitBean2((short) 0x201, "1506787111040");

        CanUnitBean canUnitBean11_null = Unit.createCanUnitBeanNull((short) 0x01, "1506787111010");
        CanUnitBean canUnitBean12_null = Unit.createCanUnitBeanNull((short) 0x22, "1506787222220");
        CanUnitBean canUnitBean13_null = Unit.createCanUnitBeanNull((short) 0x122, "1506787222230");
        CanUnitBean canUnitBean14_null = Unit.createCanUnitBeanNull((short) 0x201, "1506787222240");

        // repeat
        CanUnitBean canUnitBean11_Random = Unit.createRadomCanUnitBean((short) 0x01, "1506787111310");
        CanUnitBean canUnitBean12_Random = Unit.createRadomCanUnitBean((short) 0x22, "1506787111320");
        CanUnitBean canUnitBean13_Random = Unit.createRadomCanUnitBean((short) 0x122, "1506787111330");
        CanUnitBean canUnitBean14_Random = Unit.createRadomCanUnitBean((short) 0x201, "1506787111340");

        // repeat
        CanUnitBean canUnitBean11_1_Random = Unit.createRadomCanUnitBean((short) 0x01, "1506787111310");
        CanUnitBean canUnitBean12_1_Random = Unit.createRadomCanUnitBean((short) 0x22, "1506787111320");
        CanUnitBean canUnitBean13_1_Random = Unit.createRadomCanUnitBean((short) 0x122, "1506787111330");
        CanUnitBean canUnitBean14_1_Random = Unit.createRadomCanUnitBean((short) 0x201, "1506787111340");

        // repeat
        CanUnitBean canUnitBean11_null_Random = Unit.createRadomCanUnitBean((short) 0x01, "1506787111320");
        CanUnitBean canUnitBean12_null_Random = Unit.createRadomCanUnitBean((short) 0x22, "1506787111330");
        CanUnitBean canUnitBean13_null_Random = Unit.createRadomCanUnitBean((short) 0x122, "1506787111340");
        CanUnitBean canUnitBean14_null_Random = Unit.createRadomCanUnitBean((short) 0x201, "1506787111350");

        // 1506787222000 0x01 0x22 0x122 0x201
        TelegramHash telegramHash2 = new TelegramHash("2222", 1506787222000L);
        CanUnitBean canUnitBean21 = Unit.createCanUnitBean((short) 0x01, "1506787222010");
        CanUnitBean canUnitBean22 = Unit.createCanUnitBean((short) 0x22, "1506787222010");
        CanUnitBean canUnitBean23 = Unit.createCanUnitBean((short) 0x122, "1506787222010");
        CanUnitBean canUnitBean24 = Unit.createCanUnitBean((short) 0x201, "1506787222010");

        CanUnitBean canUnitBean21_1 = Unit.createCanUnitBean2((short) 0x01, "1506787222110");
        CanUnitBean canUnitBean22_1 = Unit.createCanUnitBean2((short) 0x22, "1506787222110");
        CanUnitBean canUnitBean23_1 = Unit.createCanUnitBean2((short) 0x122, "1506787222120");
        CanUnitBean canUnitBean24_1 = Unit.createCanUnitBean2((short) 0x201, "1506787222120");

        CanUnitBean canUnitBean21_null = Unit.createCanUnitBeanNull((short) 0x01, "1506787222210");
        CanUnitBean canUnitBean22_null = Unit.createCanUnitBeanNull((short) 0x22, "1506787222220");
        CanUnitBean canUnitBean23_null = Unit.createCanUnitBeanNull((short) 0x122, "1506787222230");
        CanUnitBean canUnitBean24_null = Unit.createCanUnitBeanNull((short) 0x201, "1506787222240");

        JavaRDD<Tuple2<TelegramHash, CanUnitBean>> rdd = sc.parallelize(Arrays.asList(

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_1),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_1),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_1),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_1),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_null),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_null),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_null),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_null),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_Random),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_1_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_1_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_1_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_1_Random),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_null_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_null_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_null_Random),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_null_Random),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean21),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean22),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean23),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean24),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean21_1),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean22_1),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean23_1),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean24_1),

                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean21_null),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean22_null),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean23_null),
                new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean24_null)), 1);

        JavaPairRDD<TelegramHash, CanUnitBean> pairRDD = JavaPairRDD.fromJavaRDD(rdd);

        System.out.println("Before function, test data ***********************");

        pairRDD.foreach(new VoidFunction<Tuple2<TelegramHash, CanUnitBean>>() {

            String deviceId = null;

            @Override
            public void call(Tuple2<TelegramHash, CanUnitBean> tp2) throws Exception {

                if (StringUtil.isEmpty(deviceId) || !tp2._1.deviceId.equals(deviceId)) {

                    deviceId = tp2._1.deviceId;
                    System.out.println(tp2._1.deviceId + ":" + tp2._1.timestamp + "-----------");
                }
                System.out.println(tp2._2.getCanId() + ":" + tp2._2.getCanTime() + ":" + tp2._2.getConvertedDataMap());
            }
        });

        System.out.println("******************************************************************************");
        System.out.println("↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓");
        System.out.println("After function, iterator test data *********************** timeInterval  10");
        System.out.println("*****************JavaPairRDD<TelegramHash, List<List<CanUnitBean>>>*************");

        JavaPairRDD<TelegramHash, List<List<CanUnitBean>>> pairRDDReducerBy10 = pairRDD
                .mapPartitionsToPair(new SortWithinTeregramWithinPartitions(10));

        pairRDDReducerBy10.foreach(new VoidFunction<Tuple2<TelegramHash, List<List<CanUnitBean>>>>() {

            @Override
            public void call(Tuple2<TelegramHash, List<List<CanUnitBean>>> tp10) throws Exception {

                System.out.println(tp10._1.deviceId + ":" + tp10._1.timestamp + "-----------");

                List<List<CanUnitBean>> listList = tp10._2;

                for (List<CanUnitBean> list : listList) {

                    System.out.println("         " + list.get(0).getCanTime() + "      ");
                    for (CanUnitBean bean : list) {
                        System.out.println(bean.getCanId() + ":" + bean.getCanTime() + ":" + bean.getCanTimeInterval()
                                + ":" + bean.getConvertedDataMap());
                    }
                }
            }
        });

        System.out.println("******************************************************************************");
        System.out.println("↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓");
        System.out.println("After function, iterator test data *****************************************");
        System.out.println("*****************JavaPairRDD<TelegramHash, List<CanUnitBean>> *************");

        JavaPairRDD<TelegramHash, List<CanUnitBean>> pairRDDReducerBy10Single = pairRDDReducerBy10.flatMapToPair(
                new PairFlatMapFunction<Tuple2<TelegramHash, List<List<CanUnitBean>>>, TelegramHash, List<CanUnitBean>>() {

                    private static final long serialVersionUID = -5389825673734449220L;

                    @Override
                    public Iterator<Tuple2<TelegramHash, List<CanUnitBean>>> call(
                            Tuple2<TelegramHash, List<List<CanUnitBean>>> tp2) throws Exception {

                        List<Tuple2<TelegramHash, List<CanUnitBean>>> tuble2List = new ArrayList<>();

                        List<CanUnitBean> canUnitBeanList = new ArrayList<CanUnitBean>();

                        for (List<CanUnitBean> beanList : tp2._2) {

                            canUnitBeanList.addAll(beanList);
                        }

                        tuble2List.add(new Tuple2<TelegramHash, List<CanUnitBean>>(tp2._1, canUnitBeanList));

                        return tuble2List.iterator();
                    }
                });

        pairRDDReducerBy10Single.foreach(new VoidFunction<Tuple2<TelegramHash, List<CanUnitBean>>>() {

            @Override
            public void call(Tuple2<TelegramHash, List<CanUnitBean>> tp10) throws Exception {

                System.out.println(tp10._1.deviceId + ":" + tp10._1.timestamp + "-----------");

                List<CanUnitBean> listList = tp10._2;

                long canTiem = 0;

                for (CanUnitBean bean : listList) {

                    if (canTiem != bean.getCanTime()) {

                        System.out.println("         " + bean.getCanTime() + "      ");
                        canTiem = bean.getCanTime();
                    }

                    System.out.println(bean.getCanId() + ":" + bean.getCanTime() + ":" + bean.getCanTimeInterval() + ":"
                            + bean.getConvertedDataMap());
                }
            }
        });
        JavaPairRDD<AvroKey, NullWritable> avroRDD= pairRDDReducerBy10Single.mapToPair(tuple ->
           new Tuple2<>(new AvroKey<>(mapper.map(tuple._2)),
           NullWritable.get()));

        avroRDD.saveAsNewAPIHadoopFile("data/arvo-out/arvo" + String.valueOf(System.currentTimeMillis()),
        		AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class, confHaddop);
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
