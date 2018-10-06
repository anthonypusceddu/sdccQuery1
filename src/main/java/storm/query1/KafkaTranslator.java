package storm.query1;


import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.costant.Costant;
import storm.entity.Sensor;

import java.io.IOException;
import java.util.List;

public class KafkaTranslator implements RecordTranslator<String, JsonNode> {


    @Override
    public List<Object> apply(ConsumerRecord<String, JsonNode> consumerRecord) {
        // Records coming from Kafka
        JsonNode record = consumerRecord.value();
        //System.err.println(record);
        Sensor sen=null;
        try {
            sen=new ObjectMapper().treeToValue(record, Sensor.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Emit current timestamp for metrics calculation
        Long startProcessingTime = System.nanoTime();
        return new Values(sen, startProcessingTime);

    }

    @Override
    public Fields getFieldsFor(String s) {
        return new Fields(Costant.F_RECORD, Costant.F_START_PROCESSING_TIME);
    }

    @Override
    public List<String> streams() {
        return DEFAULT_STREAM;
    }
}
