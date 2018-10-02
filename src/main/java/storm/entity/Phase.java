package storm.entity;

public class Phase {
    private int id;
    private int green;
    private int red;
    private double effective_green;
    private double effective_red;
    private double ratioFlowSaturation;

    public Phase(int id) {
        this.id = id;
    }
    public Phase(){

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getGreen() {
        return green;
    }

    public void setGreen(int green) {
        this.green = green;
    }

    public int getRed() {
        return red;
    }

    public void setRed(int red) {
        this.red = red;
    }

    public double getEffective_green() {
        return effective_green;
    }

    public void setEffective_green(double effective_green) {
        this.effective_green = effective_green;
    }

    public double getEffective_red() {
        return effective_red;
    }

    public void setEffective_red(double effective_red) {
        this.effective_red = effective_red;
    }

    public double getRatioFlowSaturation() {
        return ratioFlowSaturation;
    }

    public void setRatioFlowSaturation(double ratioFlowSaturation) {
        this.ratioFlowSaturation = ratioFlowSaturation;
    }
}
