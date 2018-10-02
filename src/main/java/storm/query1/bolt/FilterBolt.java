package storm.query1.bolt;

import storm.costant.Costant;
import storm.entity.Intersection;
import storm.entity.Sensor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterBolt extends BaseRichBolt {
//il filter bolt riceve semafori e invia incroci quando essi sono completi
    private OutputCollector collector;
    private HashMap<Integer, Intersection> mappa;
    private Intersection inc;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Costant.ID,Costant.INTERSECTION));
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        mappa = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        List<Sensor> list;
        //controllare integrità tupla e/o semaforo rotto
        Sensor s=(Sensor) input.getValueByField(Costant.SENSOR);
        if ( mappa.containsKey(s.getIntersection()) ){//incrocio esiste in hasmap
            Intersection c;
            c = mappa.get(s.getIntersection());//prendi incrocio dall'hashmap
            c.getL().add(s);//aggiungi il semaforo all'incrocio
            if ( c.getL().size() == Costant.SEM_INTERSEC ){//se l'incrocio è completo
                mappa.remove(s.getIntersection());//rimuovi l'incrocio dall'hashmap
                media(c);//calcola la media
                inc=c;
                collector.emit(new Values( s.getIntersection(), inc ) );//emetti l'incrocio
            }
            else{//?
                mappa.put(s.getIntersection(), inc);
            }
        }
        else {//l'incrocio non esiste in hashmap
            list = new ArrayList<>();
            list.add(s);
            inc = new Intersection(list, s.getIntersection());//crea incrocio con il semaforo ricevuto
            mappa.put(s.getIntersection(), inc );//metti in hashmap l'incrocio
        }

    }

    private void media(Intersection c) {
        double somma = 0;
        int numeroTotaleVeicoli = 0;
        for ( int i = 0 ; i<Costant.SEM_INTERSEC ; i++){
            somma += c.getL().get(i).getSpeed()*c.getL().get(i).getNumVehicles();
            numeroTotaleVeicoli += c.getL().get(i).getNumVehicles();
        }
        c.setNumeroVeicoli(numeroTotaleVeicoli);
        c.setVelocitaMedia(somma/numeroTotaleVeicoli);
    }
}