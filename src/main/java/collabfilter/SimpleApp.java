package collabfilter;

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class SimpleApp {
	public static void main(String[] args) {

		String logFile = "/home/joshua/dev/spark-1.1.0-bin-hadoop2.4/README.md";
		// some file on your system
		SparkConf conf = new SparkConf().set("spark.master", "local[4]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = countChar(logData, "a");
		long numBs = countChar(logData, "b");

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	}

	private static long countChar(JavaRDD<String> logData, String x) {
		long numAs = logData.filter(s -> s.contains(x)).count();
		return numAs;
	}
}