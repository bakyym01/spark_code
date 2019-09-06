package kafka;

public class HainiuKafkaTest {
    public static void main(String[] args) {
        HainiuKafkaProducer producer = new HainiuKafkaProducer("hainiu_test");
        HainiuKafkaConsumer consumer = new HainiuKafkaConsumer("hainiu_test");
        producer.start();
        consumer.start();
    }
}
