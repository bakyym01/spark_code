package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HainiuKafkaConsumer extends Thread{
    private String topics;
    private ConsumerConnector consumer;
    private Properties pros;
    public HainiuKafkaConsumer(String topics){
        this.topics=topics;
        this.pros=new Properties();
        pros.put("zookeeper.connect","nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181");
        pros.put("group.id","group1");
        pros.put("zookeeper.session.timeouts.ms","60000");
        this.consumer= Consumer.createJavaConsumerConnector(new ConsumerConfig(pros));
    }

    @Override
    public void run() {
        Map<String,Integer> topicCountMap=new HashMap<String,Integer>();
        topicCountMap.put(topics,1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topics).get(0);
        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
        while(iterator.hasNext()){
            System.out.println("receive:"+new String(iterator.next().message()));
            try {
                sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
