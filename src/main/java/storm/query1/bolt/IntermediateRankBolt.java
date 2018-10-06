package storm.query1.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import storm.costant.Costant;
import storm.entity.Intersection;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IntermediateRankBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.PARTIAL_RANK));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //ordina lista di incroci creando una classifica e invia classifica
        List<Intersection> list= (List<Intersection>) input.getValueByField(Costant.LIST_INTERSECTION);
        Collections.sort(list,new Intersection());
        List<Intersection> list2 = null;
        if(list.size() > Costant.TOP_K) {
            list2 = new ArrayList<>(list.subList(0, Costant.TOP_K - 1));
            collector.emit(new Values(list2));
        }else {
            collector.emit(new Values(list));
        }

    }
}