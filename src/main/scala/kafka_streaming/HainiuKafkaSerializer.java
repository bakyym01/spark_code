package kafka_streaming;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class HainiuKafkaSerializer implements Serializer {

    public void configure(Map map, boolean b) {

    }

    public byte[] serialize(String s, Object o) {
        if (o==null){
            return null;
        }else{
            byte [] bytes =null;
            ObjectOutputStream oo=null;
            ByteArrayOutputStream bo=null;
            try{
                bo =new ByteArrayOutputStream();
                oo = new ObjectOutputStream(bo);
                oo.writeObject(o);
                bytes = bo.toByteArray();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                try {
                    oo.close();
                    bo.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return bytes;
        }

    }

    public void close() {

    }
}
