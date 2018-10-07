package storm.query1;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.Builder;
import org.apache.storm.mongodb.bolt.MongoUpdateBolt;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.mongodb.common.SimpleQueryFilterCreator;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import storm.costant.Costant;
import storm.query1.bolt.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;

import java.io.InputStream;
import java.util.Properties;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;

public class Topology1 {

    private Properties properties;

    public Topology1()throws Exception{
        properties=new Properties();
        InputStream is=this.getClass().getResourceAsStream("/config.properties");
        properties.load(is);
    }
    public static void main(String[] args) throws Exception {
        new Topology1().runMain(args);
    }

    protected void runMain(String[] args) throws Exception {
        Config conf=this.getConfig();
        if (args != null && args.length > 0) {
            System.out.println("argument1=" + args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(properties.getProperty("topologyName"), conf, this.getTopologyKafkaSpout(getKafkaSpoutConfig(properties.getProperty("kafka.brokerurl"),properties.getProperty("kafka.topic"),this.properties)));
        } else {
            System.out.println("Create local cluster");
            conf.setMaxTaskParallelism(3);
            //conf.setNumWorkers(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(properties.getProperty("topologyName"), conf,this.getTopologyKafkaSpout(getKafkaSpoutConfig(properties.getProperty("kafka.brokerurl"),properties.getProperty("kafka.topic"),this.properties)));
            //shutdown the cluster
            /*Thread.sleep(15000);
             cluster.killTopology(properties.getProperty("topologyName"));
             cluster.shutdown();
             System.exit(0);*/
        }
    }

    protected Config getConfig(){
        Config config = new Config();
        config.setDebug(false);
        config.setMessageTimeoutSecs(30000);
        return config;
    }

     protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, JsonNode> spoutConfig) {
         String urlMongoDB=properties.getProperty("urlMongoDB");
         String collectionName= properties.getProperty("collectionName");
         MongoMapper mapperUpdate = new CustomMongoUpdateMapper()
                 .withFields(Costant.ID, Costant.RANK_TOPK);

         QueryFilterCreator updateQueryCreator = new SimpleQueryFilterCreator().withField(Costant.ID);

         MongoUpdateBolt updateBolt15M = new MongoUpdateBolt(urlMongoDB, collectionName, updateQueryCreator, mapperUpdate);
         MongoUpdateBolt updateBolt1H = new MongoUpdateBolt(urlMongoDB, collectionName, updateQueryCreator, mapperUpdate);
         MongoUpdateBolt updateBolt24H = new MongoUpdateBolt(urlMongoDB, collectionName, updateQueryCreator, mapperUpdate);
         //if a new document should be inserted if there are no matches to the query filter
         updateBolt15M.withUpsert(true);
         updateBolt1H.withUpsert(true);
         updateBolt24H.withUpsert(true);

        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout(Costant.KAFKA_SPOUT, new KafkaSpout(spoutConfig), Costant.NUM_SPOUT_QUERY_1);
        tp.setBolt(Costant.FILTER_QUERY_1,new FilterBolt(),Costant.NUM_FILTER_QUERY1).shuffleGrouping(Costant.KAFKA_SPOUT);

        tp.setBolt(Costant.AVG15M_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(10)),Costant.NUM_AVG15M)
                .fieldsGrouping(Costant.FILTER_QUERY_1,Costant.STREAM_15M, new Fields(Costant.ID));
        tp.setBolt(Costant.AVG1H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(20)),Costant.NUM_AVG1H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, Costant.STREAM_1H,new Fields(Costant.ID));

        tp.setBolt(Costant.AVG24H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(40)),Costant.NUM_AVG24H)
                .fieldsGrouping(Costant.FILTER_QUERY_1,Costant.STREAM_24H,new Fields(Costant.ID));

        tp.setBolt(Costant.INTERMEDIATERANK_15M, new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK15M)
                .shuffleGrouping(Costant.AVG15M_BOLT);

        tp.setBolt(Costant.INTERMEDIATERANK_1H, new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK1H)
                .shuffleGrouping(Costant.AVG1H_BOLT);

        tp.setBolt(Costant.INTERMEDIATERANK_24H, new IntermediateRankBolt(), Costant.NUM_INTERMEDIATERANK24H)
                .shuffleGrouping(Costant.AVG24H_BOLT);

        tp.setBolt(Costant.GLOBAL15M_AVG, new GlobalRankBolt(Costant.ID15M,Costant.NUM_AVG15M),Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.INTERMEDIATERANK_15M);

        tp.setBolt(Costant.GLOBAL1H_AVG, new GlobalRankBolt(Costant.ID1H,Costant.NUM_AVG1H),Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.INTERMEDIATERANK_1H);

        tp.setBolt(Costant.GLOBAL24H_AVG, new GlobalRankBolt(Costant.ID24H,Costant.NUM_AVG24H),Costant.NUM_GLOBAL_BOLT)
                .shuffleGrouping(Costant.INTERMEDIATERANK_24H);
        tp.setBolt(Costant.MONGODB15M,updateBolt15M,Costant.NUM_MONGOBOLT15M).shuffleGrouping(Costant.GLOBAL15M_AVG);
         tp.setBolt(Costant.MONGODB1H,updateBolt1H,Costant.NUM_MONGOBOLT1H).shuffleGrouping(Costant.GLOBAL1H_AVG);
         tp.setBolt(Costant.MONGODB24H,updateBolt24H,Costant.NUM_MONGOBOLT24H).shuffleGrouping(Costant.GLOBAL24H_AVG);
        return tp.createTopology();
    }
    public static KafkaSpoutConfig<String, JsonNode> getKafkaSpoutConfig(String bootstrapServers,String topicName,Properties properties) {

        KafkaTranslator kafkaTranslator = new KafkaTranslator();
        Builder<String,JsonNode> kafkaSpoutConfigBuilder = new Builder(bootstrapServers, StringDeserializer.class, org.apache.kafka.connect.json.JsonDeserializer.class, topicName);
        return kafkaSpoutConfigBuilder
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,properties.getProperty("nameConsumerGroup"))
                //.setRetry(getRetryService())
                //.setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(LATEST)
                //.setMaxUncommittedOffsets(250)
                .setRecordTranslator(kafkaTranslator)
                .build();
    }
    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
