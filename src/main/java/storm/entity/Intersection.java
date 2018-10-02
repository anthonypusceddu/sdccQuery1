package storm.entity;

import storm.costant.Costant;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

import java.util.Comparator;
import java.util.List;

public class Intersection implements Comparator<Intersection> {

    private List<Sensor> l;
    private int id;
    private double VelocitaMedia;
    private double medianaVeicoli;
    private int numeroVeicoli;
    //private TDigest td1;
    private List<Phase> phases;

    public Intersection(){

    }

    public Intersection(List<Sensor> l, int id) {
        this.l = l;
        this.id = id;
        //this.td1= new AVLTreeDigest(Costant.COMPRESSION);
    }

    public List<Sensor> getL() {
        return l;
    }

    public void setL(List<Sensor> l) {
        this.l = l;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getVelocitaMedia() {
        return VelocitaMedia;
    }

    public void setVelocitaMedia(double velocitaMedia) {
        VelocitaMedia = velocitaMedia;
    }

    public int getNumeroVeicoli() {
        return numeroVeicoli;
    }

    public void setNumeroVeicoli(int numeroVeicoli) {
        this.numeroVeicoli = numeroVeicoli;
    }

    public double getMedianaVeicoli() {
        return medianaVeicoli;
    }

    public void setMedianaVeicoli(double medianaVeicoli) {
        this.medianaVeicoli = medianaVeicoli;
    }

    /*lic TDigest getTd1() {
        return td1;
    }

    public void setTd1(TDigest td1) {
        this.td1 = td1;
    }*/

    @Override
    public String toString(){
        return "id="+this.id+" velMedia="+this.VelocitaMedia;
    }

    @Override
    public int compare(Intersection intersection, Intersection t1) {
        if ( intersection.getVelocitaMedia() > t1.getVelocitaMedia() )
            return -1;
        else if ( intersection.getVelocitaMedia() < t1.getVelocitaMedia() )
            return 1;
        else
            return 0;
    }

    public List<Phase> getPhases() {
        return phases;
    }

    public void setPhases(List<Phase> phases) {
        this.phases = phases;
    }
}
