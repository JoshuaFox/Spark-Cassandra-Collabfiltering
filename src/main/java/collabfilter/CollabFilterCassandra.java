package collabfilter;

import java.util.Comparator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.system_add_column_family;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
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

	private static VoidFunction<Tuple2<Tuple2<Integer, Integer>, Double>> x;
	private static Object yyyy;

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

			JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(rating -> new Tuple2<Object, Object>(rating.user(), rating.product()));

			JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD
					.fromJavaRDD(model
							.predict(JavaRDD.toRDD(userProducts))
							.toJavaRDD()
							.map(prediction -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(prediction.user(), prediction.product()),
									Math.round(10 * prediction.rating()) / 10.0)));
			Comparator<Tuple2<Integer, Integer>> comparator_ =new Comparator<Tuple2<Integer,Integer>>() {

				@Override
				public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
					// TODO Auto-generated method stub
					return 0;
				}
			};
			JavaPairRDD<Tuple2<Integer, Integer>, Double> sortedPredictions = predictions.sortByKey(comparator_);
			List<Tuple2<Tuple2<Integer, Integer>, Double>> collected = sortedPredictions.collect();
			Stream<String> stringified = collected.stream().map(CollabFilterCassandra::inspect);
			String string = stringified.reduce("", (a, b) -> a + "\n" + b);

			System.out.println(string);
			System.out.println();

		}
	}

	private static String inspect(Tuple2<Tuple2<Integer, Integer>, Double> pred) {
		return "User " + pred._1()._1() + " is predicted to rate item " + pred._1()._2() + " with rating " + pred._2();
	}
}