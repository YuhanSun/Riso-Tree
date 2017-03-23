package commons;

public class MyRectangle {
    public double min_x;
    public double min_y;
    public double max_x;
    public double max_y;

    public MyRectangle(double p_min_x, double p_min_y, double p_max_x, double p_max_y) {
        this.min_x = p_min_x;
        this.min_y = p_min_y;
        this.max_x = p_max_x;
        this.max_y = p_max_y;
    }

    public MyRectangle() {
        this.min_x = 0.0;
        this.min_y = 0.0;
        this.max_x = 0.0;
        this.max_y = 0.0;
    }
    
    public MyRectangle(String str)
    {
    	str = str.substring(1, str.length() - 1);
    	String [] liStrings = str.split(",");
    	this.min_x = Double.parseDouble(liStrings[0]);
    	this.min_y = Double.parseDouble(liStrings[1]);
    	this.max_x = Double.parseDouble(liStrings[2]);
    	this.max_y = Double.parseDouble(liStrings[3]);
    }
    
    @Override
    public String toString()
    {
    	String string = "";
    	string += "(" +Double.toString(min_x);
    	string += ", " +Double.toString(min_y);
    	string += ", " + Double.toString(max_x);
    	string += ", " + Double.toString(max_y) + ")";
    	return string;
    }
}