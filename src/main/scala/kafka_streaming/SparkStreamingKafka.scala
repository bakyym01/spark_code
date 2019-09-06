package kafka_streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}

import scala.collection.mutable

object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    val brokers="nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
    val topic="wangliang11"
    val sparkConf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf,Durations.seconds(5))
    val set: Set[String] = topic.split(",").toSet
    val kafkaParam = new mutable.HashMap[String,Object]()
    kafkaParam+="bootstrap.servers"->brokers
    kafkaParam+="group.id"->"console-consumer-8006"
    kafkaParam+="key.deserializer"->classOf[StringDeserializer].getName
    kafkaParam+="value.deserializer"->classOf[StringDeserializer].getName
    val offset = new mutable.HashMap[TopicPartition,Long]()
    offset+=new TopicPartition(topic,0)->1
    val value: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(set,kafkaParam,offset)
    val directDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,value)
    val reduceBykey = directDstream.flatMap(_.value().split(" ")).map((_,1)).reduceByKey(_+_)
    reduceBykey.foreachRDD(f=>{
      println(s"${f.collect().toList}")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
