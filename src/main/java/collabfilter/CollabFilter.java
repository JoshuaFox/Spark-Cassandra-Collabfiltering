package collabfilter;

import java.util.*;
import java.util.stream.Stream;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;
 

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

public class CollabFilter {
	@SuppressWarnings("serial")
	public static void main(String[] args) {
 
		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example").set("spark.master", "local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load and parse the data
		String path = "data/mllib/als/test.data";
		JavaRDD<String> data = sc.textFile(path);
		
		JavaRDD<Rating> ratings = data.map( 
			s-> {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
				}
			
		 );

		// Build the recommendation model using ALS
		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

		// Evaluate the model on rating data
		JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(r -> new Tuple2<Object, Object>(r.user(), r.product()));

 
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
				.map(r -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating())));

		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD
				.fromJavaRDD(ratings.map(r -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating()))).join(predictions).values();

		double mse = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
				pair-> {
					Double diff= pair._1() - pair._2() ;
					return (Object)(diff*diff);//squared difference between prediction and ratings
					}
				).rdd()).mean();

		System.out.println("Mean Squared Error = " + mse);
		List<Tuple2<Double, Double>> collected = ratesAndPreds.collect(); 
 
		System.out.println(collected);
	}
}