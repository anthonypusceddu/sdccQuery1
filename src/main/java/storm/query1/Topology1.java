package storm.query1;

import storm.entity.Produttore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import storm.costant.Costant;
import storm.query1.bolt.AvgBolt;
import storm.query1.bolt.FilterBolt;
import storm.query1.bolt.GlobalRankBolt;
import storm.query1.bolt.IntermediateRankBolt;
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
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.util.Properties;

public class Topology1 {

    private Properties properties;

    public static void main(String[] args) throws Exception {
        Properties fileConfig = new Properties();
        fileConfig.load(new FileInputStream("config.properties"));

        new Topology1().runMain(fileConfig);
        ///
    }

    protected void runMain(Properties fileConfig) throws Exception {
        Config tpConf = getConfig();
        System.out.println(fileConfig.get("kafka.brooker"));
        //Produttore
       // Produttore p = new Produttore();
       // p.inviaRecord();
       // p.terminaProduttore();

        // run local cluster
    /*  tpConf.setMaxTaskParallelism(Costant.NUM_PARALLELISM);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Costant.TOPOLOGY_QUERY_1, tpConf, getTopology());
*/
    //local
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Costant.TOPOLOGY_QUERY_1, tpConf,  getTopologyKafkaSpout(getKafkaSpoutConfig(fileConfig)));
        Utils.sleep(100000);
        cluster.killTopology(Costant.TOPOLOGY_QUERY_1);
        cluster.shutdown();

        //run jar
        //System.setProperty("storm.jar", "path-to-jar");
        // getTopologyKafkaSpout(getKafkaSpoutConfig(fileConfig));

       // StormSubmitter.submitTopology(Costant.TOPOLOGY_QUERY_1, tpConf, getTopologyKafkaSpout(getKafkaSpoutConfig(fileConfig)));
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(false);
        return config;
    }

   /* protected StormTopology getTopology() {
        final TopologyBuilder tp = new TopologyBuilder();

        tp.setSpout(Costant.SPOUT, new SpoutTraffic(), Costant.NUM_SPOUT_QUERY_1);

        tp.setBolt(Costant.FILTER_QUERY_1,new FilterBolt(),Costant.NUM_FILTER_QUERY1).shuffleGrouping(Costant.SPOUT);

        tp.setBolt(Costant.AVG15M_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(5)),Costant.NUM_AVG15M)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.AVG1H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(10)),Costant.NUM_AVG1H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.AVG24H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(15)),Costant.NUM_AVG24H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

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
        return tp.createTopology();
    }*/

     protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout(Costant.KAFKA_SPOUT, new KafkaSpout<>(spoutConfig), Costant.NUM_SPOUT_QUERY_1);

       /* tp.setBolt(Costant.FILTER_QUERY_1,new FilterBolt(),Costant.NUM_FILTER_QUERY1).shuffleGrouping(Costant.SPOUT);

        tp.setBolt(Costant.AVG15M_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(5)),Costant.NUM_AVG15M)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.AVG1H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(10)),Costant.NUM_AVG1H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

        tp.setBolt(Costant.AVG24H_BOLT, new AvgBolt().withTumblingWindow(Duration.seconds(15)),Costant.NUM_AVG24H)
                .fieldsGrouping(Costant.FILTER_QUERY_1, new Fields(Costant.ID));

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
                .shuffleGrouping(Costant.INTERMEDIATERANK_24H);*/
        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(Properties fileConfig){
        this.setProperties();
        return KafkaSpoutConfig
                .builder(fileConfig.getProperty("kafka.brooker"),fileConfig.getProperty("kafka.topic"))
                //.setProp(ConsumerConfig.GROUP_ID_CONFIG, fileConfig.getProperty("kafka.consumerID"))
                //.setProp(properties)
                //.setRetry(getRetryService())
                //.setOffsetCommitPeriodMs(Integer.parseInt(fileConfig.getProperty("kafka.commitOffset")))
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST)
                //.setMaxUncommittedOffsets(Integer.parseInt(fileConfig.getProperty("kafka.UncommitOffset")))
                .setRecordTranslator(new KafkaTranslator())
                .build();
    }

    protected void setProperties() {
        properties = new Properties();
        properties.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
    }
    protected KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

}
