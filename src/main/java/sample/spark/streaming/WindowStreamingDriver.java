package sample.spark.streaming;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WindowStreamingDriver {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("StreamingDriver").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        TelegramHash telegramHash1 = new TelegramHash("1111", 1506787111000L);

        CanUnitBean canUnitBean1_00_1000 = Unit.createCanUnitBean((short) 0x00, "1506787111000");
        CanUnitBean canUnitBean1_0E_1000 = Unit.createCanUnitBean((short) 0x0E, "1506787111000");

        CanUnitBean canUnitBean1_01_1000 = Unit.createCanUnitBean((short) 0x01, "1506787111000");
        CanUnitBean canUnitBean1_22_1000 = Unit.createCanUnitBean((short) 0x22, "1506787111000");
        CanUnitBean canUnitBean1_122_1000 = Unit.createCanUnitBean((short) 0x122, "1506787111000");
        CanUnitBean canUnitBean1_201_1000 = Unit.createCanUnitBean((short) 0x201, "1506787111000");

        CanUnitBean canUnitBean1_01_1010 = Unit.createCanUnitBeanNull((short) 0x01, "1506787111010");
        CanUnitBean canUnitBean1_22_1010 = Unit.createCanUnitBeanNull((short) 0x22, "1506787111010");
        CanUnitBean canUnitBean1_122_1010 = Unit.createCanUnitBeanNull((short) 0x122, "1506787111010");
        CanUnitBean canUnitBean1_201_1010 = Unit.createCanUnitBeanNull((short) 0x201, "1506787111010");

        CanUnitBean canUnitBean1_01_1020 = Unit.createCanUnitBean2((short) 0x01, "1506787111020");
        CanUnitBean canUnitBean1_22_1020 = Unit.createCanUnitBean2((short) 0x22, "1506787111020");
        CanUnitBean canUnitBean1_122_1020 = Unit.createCanUnitBean2((short) 0x122, "1506787111020");
        CanUnitBean canUnitBean1_201_1020 = Unit.createCanUnitBean2((short) 0x201, "1506787111020");

        CanUnitBean canUnitBean1_01_1030 = Unit.createCanUnitBean2((short) 0x01, "1506787111030");
        CanUnitBean canUnitBean1_22_1030 = Unit.createCanUnitBean2((short) 0x22, "1506787111030");
        CanUnitBean canUnitBean1_122_1030 = Unit.createCanUnitBean2((short) 0x122, "1506787111030");
        CanUnitBean canUnitBean1_201_1030 = Unit.createCanUnitBean2((short) 0x201, "1506787111030");

        CanUnitBean canUnitBean1_00_2000 = Unit.createCanUnitBean((short) 0x00, "1506787112000");
        CanUnitBean canUnitBean1_0E_2000 = Unit.createCanUnitBean((short) 0x0E, "1506787112000");

        CanUnitBean canUnitBean1_01_2000 = Unit.createCanUnitBean((short) 0x01, "1506787112000");
        CanUnitBean canUnitBean1_22_2000 = Unit.createCanUnitBean((short) 0x22, "1506787112000");
        CanUnitBean canUnitBean1_122_2000 = Unit.createCanUnitBean((short) 0x122, "1506787112000");
        CanUnitBean canUnitBean1_201_2000 = Unit.createCanUnitBean((short) 0x201, "1506787112000");

        CanUnitBean canUnitBean1_01_2010 = Unit.createCanUnitBeanNull((short) 0x01, "1506787112010");
        CanUnitBean canUnitBean1_22_2010 = Unit.createCanUnitBeanNull((short) 0x22, "1506787112010");
        CanUnitBean canUnitBean1_122_2010 = Unit.createCanUnitBeanNull((short) 0x122, "1506787112010");
        CanUnitBean canUnitBean1_201_2010 = Unit.createCanUnitBeanNull((short) 0x201, "1506787112010");

        CanUnitBean canUnitBean1_00_3000 = Unit.createCanUnitBean((short) 0x00, "1506787113000");
        CanUnitBean canUnitBean1_0E_3000 = Unit.createCanUnitBean((short) 0x0E, "1506787113000");

        CanUnitBean canUnitBean1_01_3010 = Unit.createCanUnitBean((short) 0x01, "1506787113010");
        CanUnitBean canUnitBean1_22_3020 = Unit.createCanUnitBean((short) 0x22, "1506787113020");
        CanUnitBean canUnitBean1_122_3030 = Unit.createCanUnitBean((short) 0x122, "1506787113030");
        CanUnitBean canUnitBean1_201_3040 = Unit.createCanUnitBean((short) 0x201, "1506787113040");

        // 1506787222000 0x01 0x22 0x122 0x201
        TelegramHash telegramHash2 = new TelegramHash("2222", 1506787222000L);

        CanUnitBean canUnitBean2_00_2000 = Unit.createCanUnitBean((short) 0x00, "1506787222010");
        CanUnitBean canUnitBean2_0E_2000 = Unit.createCanUnitBean((short) 0x0E, "1506787222010");

        CanUnitBean canUnitBean2_01_2000 = Unit.createCanUnitBean((short) 0x01, "1506787222010");
        CanUnitBean canUnitBean2_22_2000 = Unit.createCanUnitBean((short) 0x22, "1506787222010");
        CanUnitBean canUnitBean2_122_2000 = Unit.createCanUnitBean((short) 0x122, "1506787222010");
        CanUnitBean canUnitBean2_201_2000 = Unit.createCanUnitBean((short) 0x201, "1506787222010");

        CanUnitBean canUnitBean2_00_3000 = Unit.createCanUnitBean((short) 0x00, "1506787223010");
        CanUnitBean canUnitBean2_0E_3000 = Unit.createCanUnitBean((short) 0x0E, "1506787223010");

        CanUnitBean canUnitBean2_01_3000 = Unit.createCanUnitBean((short) 0x01, "1506787223010");
        CanUnitBean canUnitBean2_22_3000 = Unit.createCanUnitBean((short) 0x22, "1506787223010");
        CanUnitBean canUnitBean2_122_3000 = Unit.createCanUnitBean((short) 0x122, "1506787223010");
        CanUnitBean canUnitBean2_201_3000 = Unit.createCanUnitBean((short) 0x201, "1506787223010");

        CanUnitBean canUnitBean2_00_4000 = Unit.createCanUnitBean((short) 0x00, "1506787224010");
        CanUnitBean canUnitBean2_0E_4000 = Unit.createCanUnitBean((short) 0x0E, "1506787224010");

        CanUnitBean canUnitBean2_01_4010 = Unit.createCanUnitBean((short) 0x01, "1506787224010");
        CanUnitBean canUnitBean2_22_4020 = Unit.createCanUnitBean((short) 0x22, "1506787224020");
        CanUnitBean canUnitBean2_122_4030 = Unit.createCanUnitBean((short) 0x122, "1506787224030");
        CanUnitBean canUnitBean2_201_4040 = Unit.createCanUnitBean((short) 0x201, "1506787224040");

        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.milliseconds(1000));

        JavaRDD<Tuple2<TelegramHash, CanUnitBean>> rdd = jssc.sparkContext()
                .parallelize(Arrays.asList(

                        // 1506787111000
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_00_1000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_0E_1000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_1000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_1000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_1000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_1000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_1010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_1010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_1010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_1010),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_1020),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_1020),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_1020),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_1020),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_1030),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_1030),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_1030),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_1030),

                        // 1506787113000 1506787113000 1506787113000
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_00_3000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_0E_3000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_3010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_3020),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_3030),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_3040),

                        // 1506787112000
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_00_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_0E_2000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_2000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_01_2010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_22_2010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_122_2010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean1_201_2010),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_00_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_0E_2000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_01_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_22_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_122_2000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_201_2000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_00_3000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_0E_3000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_01_3000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_22_3000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_122_3000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_201_3000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_00_4000),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_0E_4000),

                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_01_4010),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_22_4020),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_122_4030),
                        new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean2_201_4040)));

        Queue<JavaRDD<Tuple2<TelegramHash, CanUnitBean>>> queue = new ArrayDeque<>();
        queue.add(rdd);
        queue.add(rdd);
        queue.add(rdd);
        queue.add(rdd);

        JavaDStream<Tuple2<TelegramHash, CanUnitBean>> inputJavaDStream = jssc.queueStream(queue);

        System.out.println("************inputJavaDStream ********************************");
        System.out.println("************************************************************");

        JavaPairDStream<TelegramHash, List<CanUnitBean>> inputListJavaDStream = inputJavaDStream
                .mapPartitionsToPair(new ChangeDataOutputFunction());

        JavaPairDStream<TelegramHash, List<CanUnitBean>> inputListJavaDStreamWindow = inputListJavaDStream
                .window(Durations.milliseconds(3000), Durations.milliseconds(1000));

        JavaPairDStream<TelegramHash, List<CanUnitBean>> outputJavaPairDStream = inputListJavaDStreamWindow
                .mapPartitionsToPair(new ChangeDataStreamingFunction());

        outputJavaPairDStream.print();

        jssc.start();

        jssc.awaitTermination();
    }

}
