package storm.query1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

public class KafkaTranslator implements RecordTranslator<String, String> {

    public static final String F_RECORD = "record";
    public static final String F_START_PROCESSING_TIME = "time";

    @Override
    public List<Object> apply(ConsumerRecord<String, String> consumerRecord) {
        // Records coming from Kafka
        String record = null;
        try {
            record = consumerRecord.value();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Emit current timestamp for metrics calculation
        Long startProcessingTime = System.nanoTime();
        System.err.println(record);
        return new Values(record, startProcessingTime);
    }

    @Override
    public Fields getFieldsFor(String s) {
        return new Fields(F_RECORD, F_START_PROCESSING_TIME);
    }

    @Override
    public List<String> streams() {
        return DEFAULT_STREAM;
    }
}
