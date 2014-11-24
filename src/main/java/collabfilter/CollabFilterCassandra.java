package collabfilter;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class CollabFilterCassandra {

	public static void main(String[] args) {

		String sparkMaster = "local[4]";
		String cassandraHost = "localhost";
		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering with Cassandra").set("spark.master", sparkMaster).set("spark.cassandra.connection.host", cassandraHost);

		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		try (Session session = connector.openSession()) {
			CassandraJavaRDD<CassandraRow> cassRowRDD = javaFunctions(sc).cassandraTable("employerratings", "ratings");
			JavaRDD<Rating> ratings = cassRowRDD.map(cassRow -> new Rating(cassRow.getInt("user_id"), cassRow.getInt("item_id"), cassRow.getDouble("rating")));

			int rank = 10;
			int numIterations = 30;
			MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

			JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(r -> new Tuple2<Object, Object>(r.user(), r.product()));

			JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
					.map(r -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating())));

			JavaRDD<Tuple2<Double, Double>> ratingsAndPredictions = JavaPairRDD
					.fromJavaRDD(ratings.map(r -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating()))).join(predictions).values();

			double mse = JavaDoubleRDD.fromRDD(ratingsAndPredictions.map(pair -> {
				Double diff = pair._1() - pair._2();
				return (Object) (diff * diff);
				// squared difference between prediction and input-ratings
				}).rdd()).mean();

			List<Tuple2<Double, Double>> collected = ratingsAndPredictions.collect();
			System.out.println("Mean Squared Error = " + mse);
			System.out.println("output and input" + collected);
		}
	}
}