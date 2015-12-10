package com.hh.spark.erji

import kafka.serializer.StringDecoder
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
      System.err.println("Usage: KafkaWordCount"
        + "<zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args // receive kafka settings

    /* create application driver StreamingContext */
    val sparkConf = new SparkConf() // define spark app's name and master type
      .setAppName("KafkaWordCount")
    //      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(10)) // set the batch duration to 10 seconds
                                                           // one job will be submit every 10 seconds

    /* create InputDStream */
    val receiversNum = 2 // set the receiver number this app will use
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.commit.enable" -> "false")
    val kafkaStreams = (1 to receiversNum.toInt).map {
      i => KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2)
    } // create one InputDStream for one receiver

    /* define operations on DStream */
    val lines = ssc.union(kafkaStreams).map(_._2) // union all InputDStream to one
    val words = lines.flatMap(_.split(" ")) // split the words in one line and flat them
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _) // count the words
    wordCounts.print // print the first ten lines of the result

    /* start the streaming job */
    ssc.start() // start
    ssc.awaitTermination() // wait, keep the app running forever
  }

}