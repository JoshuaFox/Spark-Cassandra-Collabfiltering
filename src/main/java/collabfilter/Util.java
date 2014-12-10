package collabfilter;

public class Util {
	private Util(){
		
	}

	public static double round(double x) {
		return round(x, 1);
	}
	
	public static double round(double x, int places) {
		double factor = Math.pow(10, places);
		return Math.round(factor * x) / factor;
	}

}
