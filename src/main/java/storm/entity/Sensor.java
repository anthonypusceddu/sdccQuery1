package storm.entity;


public class Sensor {

    private int intersection;
    private int trafficLight;
    private double speed;
    private int numVehicles;
    private double saturation;

    public Sensor(int i, int s, double vel, int nv){
        this.intersection = i;
        this.trafficLight = s;
        this.speed = vel;
        this.numVehicles = nv;
    }

    public Sensor(){
    }



    public int getIntersection() {
        return intersection;
    }

    public void setIntersection(int intersection) {
        this.intersection = intersection;
    }

    public int getTrafficLight() {
        return trafficLight;
    }

    public void setTrafficLight(int trafficLight) {
        this.trafficLight = trafficLight;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getNumVehicles() {
        return numVehicles;
    }

    public void setNumVehicles(int numVehicles) {
        this.numVehicles = numVehicles;
    }


    @Override
    public String toString(){
        return "Incrocio : "+ this.intersection + "semaforo : " + this.trafficLight + "velocità : "+this.speed +" num :"+this.numVehicles;
    }

    public double getSaturation() {
        return saturation;
    }

    public void setSaturation(double saturation) {
        this.saturation = saturation;
    }
}
