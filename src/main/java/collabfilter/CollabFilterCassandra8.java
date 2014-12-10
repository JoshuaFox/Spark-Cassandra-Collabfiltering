package collabfilter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

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
 * This implementation complies with Java 8. The main difference is the use of
 * lambdas instead of explicit classes for Spark functions.
 */
public class CollabFilterCassandra8 implements ICollabFilterCassandra {
	private static int ITER = 20;
	private static int RANK = 6;
	private static double LAMBDA = 0.01;

	public MatrixFactorizationModel train(JavaSparkContext sparkCtx, CassandraConnector cassandraConnector) {
		CassandraJavaRDD<CassandraRow> trainingRdd = javaFunctions(sparkCtx).cassandraTable(RatingDO.EMPLOYERRATINGS_KEYSPACE, RatingDO.RATINGS_TABLE);
		JavaRDD<Rating> trainingJavaRdd = trainingRdd.map(trainingRow -> new Rating(trainingRow.getInt(RatingDO.USER_COL), trainingRow.getInt(RatingDO.PRODUCT_COL), trainingRow.getDouble(RatingDO.RATING_COL)));
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingJavaRdd), RANK, ITER, LAMBDA);
		return model;
	}

	public JavaRDD<Rating> predict(MatrixFactorizationModel model, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		RDD<Tuple2<Object, Object>> validationsRdd = JavaRDD.toRDD(validationsCassRdd.map(validationRow -> new Tuple2<Object, Object>(validationRow.getInt(RatingDO.USER_COL), validationRow.getInt(RatingDO.PRODUCT_COL))));
		JavaRDD<Rating> predictionJavaRdd = model.predict(validationsRdd).toJavaRDD();
		return predictionJavaRdd;
	}

	public double calculateRootMeanSquareError(JavaRDD<Rating> predictionJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictionsJavaPairs = JavaPairRDD.fromJavaRDD(predictionJavaRdd.map(pred -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(pred.user(), pred.product()), pred.rating())));
		JavaRDD<Rating> validationRatings = validationsCassRdd.map(validation -> new Rating(validation.getInt(RatingDO.USER_COL), validation.getInt(RatingDO.PRODUCT_COL), validation.getInt(RatingDO.RATING_COL)));
		JavaRDD<Tuple2<Double, Double>> validationAndPredictions = JavaPairRDD.fromJavaRDD(validationRatings.map(validationRating -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(validationRating.user(), validationRating.product()), validationRating.rating()))).join(predictionsJavaPairs).values();

		double meanSquaredError = JavaDoubleRDD.fromRDD(validationAndPredictions.map(pair -> {
			Double err = pair._1() - pair._2();
			return (Object) (err * err);// No covariance! Need to cast to Object
			}).rdd()).mean();
		double rmse = Math.sqrt(meanSquaredError);
		return rmse;

	}

	public String resultsReport(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd, double rmse) {
		return "User\tProduct\tPredicted\tActual\tError?\n" + predictionString(predJavaRdd, validationsCassRdd) + "\n" + "RMSE = " + Util.round(rmse, 2);
	}

	private String predictionString(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		java.util.function.Function<CassandraRow, Tuple2<Integer, Integer>> keyMapper = validationRow -> new Tuple2<Integer, Integer>(validationRow.getInt(RatingDO.USER_COL), validationRow.getInt(RatingDO.PRODUCT_COL));
		java.util.function.Function<CassandraRow, Double> valueMapper = validationRow -> validationRow.getDouble(RatingDO.RATING_COL);
		java.util.Map<Tuple2<Integer, Integer>, Double> validationMap = validationsCassRdd.collect().stream().collect(Collectors.toMap(keyMapper, valueMapper));

		java.util.function.Function<Rating, String> stringMapper = prediction -> {
			double validationRating = validationMap.get(new Tuple2<Integer, Integer>(prediction.user(), prediction.product()));
			String errWarningString = Math.abs(validationRating - prediction.rating()) >= 1 ? "ERR" : "OK";
			return prediction.user() + "\t" + prediction.product() + "\t" + Util.round(prediction.rating()) + "\t\t" + Util.round(validationRating) + "\t" + errWarningString;
		};
		Stream<Rating> sortedPredictions = predJavaRdd.collect().stream().sorted((o1, o2) -> o1.user() == o2.user() ? o1.product() - o2.product() : o1.user() - o2.user());
		String ret = sortedPredictions.map(stringMapper).collect(Collectors.joining("\n"));

		return ret;
	}

}