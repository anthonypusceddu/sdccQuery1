package storm.query1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.costant.Costant;
import storm.entity.Sensor;

import java.io.IOException;
import java.util.List;

public class KafkaTranslator implements RecordTranslator<String, String> {


    @Override
    public List<Object> apply(ConsumerRecord<String, String> consumerRecord) {
        // Records coming from Kafka
        //Sensor record = null;
        String record = consumerRecord.value();
        Sensor sen=null;
        try {
            sen=new ObjectMapper().readValue(record,Sensor.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.err.println(sen);
        try {

            //record = (Sensor) obj.treeToValue(consumerRecord.value());
           // System.err.println(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Emit current timestamp for metrics calculation
        Long startProcessingTime = System.nanoTime();
        //System.err.println(record);
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
