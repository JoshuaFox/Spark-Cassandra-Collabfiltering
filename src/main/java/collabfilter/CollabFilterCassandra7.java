package collabfilter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

/**
 * This implementation complies with Java 7. The main difference is the need for
 * explicit classes (usually anonymous local classes) for Spark functions; this
 * version also requires declaration for variables as final whenever they are to
 * be used in an inner class.
 */
public class CollabFilterCassandra7 implements ICollabFilterCassandra, Serializable {
	// Implements Serializable because anonymous inner classes hold a reference
	// to their outer class, even when that's not needed. In Java 8, lambdas are
	// free of this reference so no need to implement Serializable.
	private static int ITER = 20;
	private static int RANK = 6;
	private static double LAMBDA = 0.01;

	public MatrixFactorizationModel train(JavaSparkContext sparkCtx, CassandraConnector cassandraConnector) {
		CassandraJavaRDD<CassandraRow> trainingRdd = javaFunctions(sparkCtx).cassandraTable(RatingDO.EMPLOYERRATINGS_KEYSPACE, RatingDO.RATINGS_TABLE);
		JavaRDD<Rating> trainingJavaRdd = trainingRdd.map(new org.apache.spark.api.java.function.Function<CassandraRow, Rating>() {
			@Override
			public Rating call(CassandraRow trainingRow) throws Exception {
				return new Rating(trainingRow.getInt(RatingDO.USER_COL), trainingRow.getInt(RatingDO.PRODUCT_COL), trainingRow.getDouble(RatingDO.RATING_COL));
			}
		});
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingJavaRdd), RANK, ITER, LAMBDA);
		return model;
	}

	public JavaRDD<Rating> predict(MatrixFactorizationModel model, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		RDD<Tuple2<Object, Object>> validationsRdd = JavaRDD.toRDD(validationsCassRdd.map(new org.apache.spark.api.java.function.Function<CassandraRow, Tuple2<Object, Object>>() {
			@Override
			public Tuple2<Object, Object> call(CassandraRow validationRow) throws Exception {
				return new Tuple2<Object, Object>(validationRow.getInt(RatingDO.USER_COL), validationRow.getInt(RatingDO.PRODUCT_COL));
			}
		}));
		JavaRDD<Rating> predictionJavaRdd = model.predict(validationsRdd).toJavaRDD();
		return predictionJavaRdd;
	}

	public double calculateRootMeanSquareError(JavaRDD<Rating> predictionJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictionsJavaPairs = JavaPairRDD.fromJavaRDD(predictionJavaRdd.map(new org.apache.spark.api.java.function.Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
			@Override
			public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating pred) throws Exception {
				return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(pred.user(), pred.product()), pred.rating());
			}
			//
		}));
		JavaRDD<Rating> validationRatings = validationsCassRdd.map(new org.apache.spark.api.java.function.Function<CassandraRow, Rating>() {
			@Override
			public Rating call(CassandraRow validation) throws Exception {
				return new Rating(validation.getInt(RatingDO.USER_COL), validation.getInt(RatingDO.PRODUCT_COL), validation.getInt(RatingDO.RATING_COL));
			}
		
		});
		JavaRDD<Tuple2<Double, Double>> validationAndPredictions = JavaPairRDD.fromJavaRDD(validationRatings.map(new org.apache.spark.api.java.function.Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
		
			@Override
			public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating validationRating) throws Exception {
				return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(validationRating.user(), validationRating.product()), validationRating.rating());
			}
		
		})).join(predictionsJavaPairs).values();
		
		double meanSquaredError = JavaDoubleRDD.fromRDD(validationAndPredictions.map(new org.apache.spark.api.java.function.Function<Tuple2<Double, Double>, Object>() {
			@Override
			public Object call(Tuple2<Double, Double> pair) throws Exception {
				Double err = pair._1() - pair._2();
				return (Object) (err * err);// No covariance! Need to cast
			}
		}).rdd()).mean();
		double rmse = Math.sqrt(meanSquaredError);
		return rmse;
		 
	}

	@Override
	public String resultsReport(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd, double rmse) {
		return "User\tProduct\tPredicted\tActual\tError?\n" + predictionString(predJavaRdd, validationsCassRdd) + "\n" + "RMSE = " + Util.round(rmse, 2);
		
	}

	private String predictionString(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		java.util.function.Function<CassandraRow, Tuple2<Integer, Integer>> keyMapper = new java.util.function.Function<CassandraRow, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> apply(CassandraRow validationRow) {
				return new Tuple2<Integer, Integer>(validationRow.getInt(RatingDO.USER_COL), validationRow.getInt(RatingDO.PRODUCT_COL));
			}
		};
		java.util.function.Function<CassandraRow, Double> valueMapper = new java.util.function.Function<CassandraRow, Double>() {
			@Override
			public Double apply(CassandraRow validationRow) {
				return validationRow.getDouble(RatingDO.RATING_COL);
			}
		};

		final java.util.Map<Tuple2<Integer, Integer>, Double> validationMap = validationsCassRdd.collect().stream().collect(Collectors.toMap(keyMapper, valueMapper));

		java.util.function.Function<Rating, String> stringMapper = new java.util.function.Function<Rating, String>() {
			@Override
			public String apply(Rating prediction) {
				double validationRating = validationMap.get(new Tuple2<Integer, Integer>(prediction.user(), prediction.product()));
				String errWarningString = Math.abs(validationRating - prediction.rating()) >= 1 ? "ERR" : "OK";
				return prediction.user() + "\t" + prediction.product() + "\t" + Util.round(prediction.rating()) + "\t\t" + Util.round(validationRating) + "\t" + errWarningString;
			}
		};

		Stream<Rating> sortedPredictions = predJavaRdd.collect().stream().sorted(new Comparator<Rating>() {
			public int compare(Rating o1, Rating o2) {
				return o1.user() == o2.user() ? o1.product() - o2.product() : o1.user() - o2.user();
			}
		});
		String ret = sortedPredictions.map(stringMapper).collect(Collectors.joining("\n"));

		return ret;
	}
}