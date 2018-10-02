package storm.query1.bolt;

import storm.costant.Costant;
import storm.entity.Intersection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IntermediateRankBolt extends BaseRichBolt {

    private OutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.PARTIAL_RANK));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {//riceve lista di incroci
        //ordina lista di incroci creando una classifica e invia classifica
        List<Intersection> list= (List<Intersection>) input.getValueByField(Costant.LIST_INTERSECTION);
        Collections.sort(list,new Intersection());
        if(list.size() > Costant.TOP_K)
            list= list.subList(0, Costant.TOP_K-1);
        collector.emit(new Values(list));
    }
}