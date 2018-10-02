package storm.query1.bolt;

import storm.costant.Costant;
import storm.entity.Intersection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvgBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.LIST_INTERSECTION));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(TupleWindow inputWindow) {
        HashMap<Integer, Intersection> mappa = new HashMap<>();
        List<Tuple> tupleList = inputWindow.get();//ottieni la lista di tuple in finestra
        for ( Tuple t : tupleList){
            Intersection l = (Intersection) t.getValueByField(Costant.INTERSECTION);
            if(mappa.containsKey(l.getId())){//se la mappa contiene l'incrocio
                mappa.put(l.getId(), processAvg(mappa.get(l.getId()),l));//aggiorna la media
            }
            else{//la mappa non contiene l'incrocio,aggiungi nella mappa l'incrocio
                mappa.put(l.getId(),l);
            }
        }
        //dalla mappa ogni avg bolt avrà vari incroci e li deve raggruppare per ottenere una classifica
        List<Intersection> classifica = new ArrayList<>();
        for (Integer i : mappa.keySet() ) {
            classifica.add(mappa.get(i));//aggiungi l'incrocio nell hashmap
        }
        mappa.clear();
        collector.emit(new Values(classifica));//emetti la classifica
    }

    private Intersection processAvg(Intersection oldi, Intersection i){//aggiorna la media pesata tra 2 incroci
        int nTot = oldi.getNumeroVeicoli()+i.getNumeroVeicoli();
        double app=oldi.getVelocitaMedia()*oldi.getNumeroVeicoli()+i.getVelocitaMedia()*i.getNumeroVeicoli();
        i.setVelocitaMedia(app/nTot);
        i.setNumeroVeicoli(nTot);
        return i;
    }


}

