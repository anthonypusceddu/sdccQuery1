package storm.entity;

/*
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
*/
import storm.entity.Sensor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Produttore {
   /* private Properties props;
    private Producer<String, JsonNode> producer ;
    private ObjectMapper objectMapper;

    public Produttore(){
        props = new Properties();
        this.setProperties();
        producer = new KafkaProducer<String, JsonNode>(props);
        objectMapper = new ObjectMapper();
    }

    protected void setProperties(){
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");

    }

    public void inviaRecord(){
        Random r = new Random();

        float max = 100F;
        float min = 0F;

        for (int i = 0; i < 10; i++) {
            for ( int j = 0 ; j < 4 ; j++){
                JsonNode json = objectMapper.valueToTree(new Sensor(i,j,min + r.nextFloat() * (max - min), ThreadLocalRandom.current().nextInt(0, 100 + 1) ) ) ;
                producer.send(new ProducerRecord<>("classifica", json)  );
            }

        }
    }

    public void terminaProduttore(){
        producer.close();
    }

    public static void main(String[] args){
        Produttore p = new Produttore();
        p.inviaRecord();
        p.terminaProduttore();
    }*/
}
