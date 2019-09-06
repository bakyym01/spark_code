package kafka_streaming

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.Properties

import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import streaming.StreamingStatusValue

import scala.actors.Actor
import scala.collection.mutable

class HainiuProducerObject(val topic:String) extends Actor{
  var producer:KafkaProducer[String,StreamingStatusValue]=_;
  def init(): HainiuProducerObject ={
    val props = new Properties()
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
    props.put("key.serializer",classOf[StringSerializer].getName)
    props.put("value.serializer",classOf[HainiuKafkaSerializer].getName)
    this.producer=new KafkaProducer[String,StreamingStatusValue](props)
    this
  }

  override def act(): Unit = {
    var num:Int=1
    while (true){
      val messageStr:StreamingStatusValue=StreamingStatusValue(num)
      println("send:"+messageStr)
      producer.send(new ProducerRecord[String,StreamingStatusValue](this.topic,messageStr))
      num+=1
      if(num>10){
        num =0
      }
      Thread.sleep(3000)
    }
  }
}
object HainiuProducerObject{
  def apply(topic:String): HainiuProducerObject = new HainiuProducerObject(topic).init()
}
class HainiuConsumerObject(val topic:String) extends Actor {
  var consumer: ConsumerConnector = _

  def init() = {
    val props = new Properties()
    props.put("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181")
    props.put("group.id", "group3")
    props.put("zookeeper.session.timeout.ms", "60000")
    this.consumer = Consumer.create(new ConsumerConfig(props))
    this
  }

  override def act(): Unit = {
    val topicsCountMap:mutable.HashMap[String,Int]=mutable.HashMap()
    topicsCountMap+= topic->1
    val map: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicsCountMap)
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = map.get(this.topic).get(0)
    val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
    while(iterator.hasNext()){
      import scala.util.control.Breaks._
      breakable{
        val bytes = iterator.next().message()
        if(bytes==null){
          break()
        }
        val bi:ByteArrayInputStream=new ByteArrayInputStream(bytes)
        val oi = new ObjectInputStream(bi)
        val obj = oi.readObject()
        bi.close()
        oi.close()
        val statusValue = obj.asInstanceOf[StreamingStatusValue]
        println("receive:"+statusValue)
        Thread.sleep(1)
      }
    }
  }
}
object HainiuConsumerObject{
  def apply(topic:String): HainiuConsumerObject = new HainiuConsumerObject(topic).init()
}

object HainiuObjectTest{
  def main(args: Array[String]): Unit = {
    val producer=HainiuProducerObject("hainiu_obj")
    val consumer = HainiuConsumerObject("hainiu_obj")
    producer.start()
    consumer.start()
  }
}