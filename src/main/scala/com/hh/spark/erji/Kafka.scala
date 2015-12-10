package com.hh.spark.erji

//import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object Kafka {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf()
      .setAppName("KafkaWordCount")
      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val topicMap = topics.split(",")
      .map((_, numThreads.toInt))
      .toMap
    val kafkaStreams = KafkaUtils
      .createStream(ssc, zkQuorum, group, topicMap, 
          StorageLevel.MEMORY_AND_DISK_SER_2)   
    val lines = kafkaStreams.map(_._2)
    val words = lines.flatMap(_.split(" "))
    words.cache
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey((x:Long, y:Long) => x + y, 2)
    wordCounts.persist(StorageLevel.DISK_ONLY)
    wordCounts.print
    ssc.start()
    ssc.awaitTermination()
  }
}