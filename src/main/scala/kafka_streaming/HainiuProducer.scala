package kafka_streaming

import java.util.Properties

import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.actors.Actor
import scala.collection.mutable

class HainiuProducer(val topics:String) extends Actor{
  var producer:KafkaProducer[String,String]=_
  def init:HainiuProducer={
    val props = new Properties()
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    props.put("key.serializer",classOf[StringSerializer].getName)
    props.put("value.serializer",classOf[StringSerializer].getName)
    this.producer=new KafkaProducer[String,String](props)
    this
  }

  override def act(): Unit = {
    var num:Int=1
    while (true){
      val messageStr:String=new String("hainiu_"+num)
      println("send:"+messageStr)
      producer.send(new ProducerRecord[String,String](this.topics,messageStr))
      num+=1
      if(num>10){
        num =0
      }
      Thread.sleep(3000)
    }
  }
}
object HainiuProducer{
  def apply(topics:String): HainiuProducer = new HainiuProducer(topics).init
}
class HainiuConsumer(val topics:String) extends Actor {
  var consumer:ConsumerConnector=_
  def init()={
    val props = new Properties()
    props.put("zookeeper.connect","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    props.put("group.id","group2")
    props.put("zookeeper.session.timeout.ms","60000")
    this.consumer= Consumer.create(new ConsumerConfig(props))
    this
  }

  override def act(): Unit = {
    val topicsCountMap:mutable.HashMap[String,Int]=mutable.HashMap()
    topicsCountMap+= topics->1
    val map: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicsCountMap)
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = map.get(this.topics).get(0)
    val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
    while(iterator.hasNext()){
      println("receive:"+ new String(iterator.next().message()))
      Thread.sleep(1)
    }
  }
}
object HainiuConsumer{
  def apply(topics: String): HainiuConsumer = new HainiuConsumer(topics).init()
}

object HainiuTest{
  def main(args: Array[String]): Unit = {
    val producer = HainiuProducer("hainiu_test")
    val consumer = HainiuConsumer("hainiu_test")
    producer.start()
    consumer.start()
  }
}
