package storm.spout;

import storm.costant.Costant;
import storm.entity.Sensor;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SpoutTraffic extends BaseRichSpout {
    //invia le tuple del sensore del semaforo provenienti dagli incroci
    private SpoutOutputCollector collector;
    private Random rand;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector=spoutOutputCollector;
        rand=new Random();
    }

    @Override
    public void nextTuple() {
        //metodo per inviare tuple ai bolt
        double max = 100;
        double min = 0;
        Sensor s;
        for (int i = 0; i < Costant.N_INTERSECTIONS; i++) {
            for ( int j = 0 ; j < Costant.SEM_INTERSEC ; j++){
                s=new Sensor(i,j,min + rand.nextDouble() * (max - min), ThreadLocalRandom.current().nextInt(0, 100 + 1)) ;
                //System.out.println(s);
                collector.emit(new Values(s));
            }
        }
        System.out.println("fine generazione");
        Utils.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Costant.SENSOR));
    }
}
