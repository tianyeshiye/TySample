package sample.spark.streaming;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class JavaQueueStream {

    public static void main(String[] args) throws Exception {

     // StreamingExamples.setStreamingLogLevels();
      SparkConf sparkConf = new SparkConf().setAppName("JavaQueueStream").setMaster("local[2]");

      // Create the context
      JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(500));

      // Create the queue through which RDDs can be pushed to
      // a QueueInputDStream

      SynchronousQueue<JavaRDD<Integer>> rddQueue = new SynchronousQueue<>();
      
     // Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
      
 //     val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
      
      for (int i = 0; i < 8; i++) {
        rddQueue.add(ssc.sparkContext().parallelize(getRDD(i +1)));
      }

      // Create the QueueInputDStream and use it do some processing
      JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
      JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(
          i -> new Tuple2<>(i , 1));
//      JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
//          (i1, i2) -> i1 + i2);
      
      JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKeyAndWindow(
              (i1, i2) -> (i1 + i2) , Duration.apply(3000), Duration.apply(2000));
      
      
     // val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(args(2).toInt), Seconds(args(3).toInt))

      reducedStream.print();
      ssc.start();
      ssc.awaitTermination();
      
      for (int i = 0; i < 8; i++) {
          rddQueue.add(ssc.sparkContext().parallelize(getRDD(i +1)));
        }

    }
    
    private static List<Integer> getRDD(int max) {
    
    
    List<Integer> list = new ArrayList<>();
      for (int i = max*10 - 10; i < max*10-9; i++) {
        list.add(i);
      }
      
      return list;
    }
  }