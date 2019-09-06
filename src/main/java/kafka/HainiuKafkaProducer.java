package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class HainiuKafkaProducer extends Thread {
    private KafkaProducer<String,String>producer;
    private String topics;
    private Properties props;
    public HainiuKafkaProducer(String topics){
        props=new Properties();
        props.put("bootstrap.servers","nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer",StringSerializer.class.getName());
        this.producer=new KafkaProducer<String, String>(props);
        this.topics=topics;
    }

    @Override
    public void run() {
        int num=1;
        while(true){
            String messagestr=new String("hainiu"+num);
            System.out.println("send:"+messagestr);
            this.producer.send(new ProducerRecord<String, String>(this.topics,messagestr));
            num ++;
            if(num==10){
                num=0;
            }
            try {
                this.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
