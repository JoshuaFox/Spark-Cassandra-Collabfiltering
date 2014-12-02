package collabfilter.java7;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

import dataobject.RatingDO;

public class CollabFilterCassandra {

	private static final String INPUT_SET = "I,";
	private static final String VALIDATION_SET = "V,";
	private static final String LOCALHOST = "localhost";
	private static final String LOCAL_MASTER = "local[4]";

	private static final String EMPLOYERRATINGS_KEYSPACE = "employerratings";
	private static final String RATINGS_TABLE = "ratings";
	private static final String VALIDATION_TABLE = "validation";

	private static final String USER_COL = "user";
	private static final String PRODUCT_COL = "product";
	private static final String RATING_COL = "rating";

	private static final String SPARK_MASTER = LOCAL_MASTER;
	private static final String CASSANDRA_HOST = LOCALHOST;
	private static final String RATINGS_CSV = "data/csv/ratings.csv";

	private static int ITER = 20;
	private static int RANK = 6;
	private static double LAMBDA = 0.01;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering with Cassandra").set("spark.master", SPARK_MASTER).set("spark.cassandra.connection.host", CASSANDRA_HOST);
		JavaSparkContext sparkCtx = new JavaSparkContext(conf);
		MatrixFactorizationModel model = trainAndValidate(sparkCtx);
	}

	private static MatrixFactorizationModel trainAndValidate(JavaSparkContext sparkCtx) {
		CassandraConnector cassandraConnector = CassandraConnector.apply(sparkCtx.getConf());
		try (Session session = cassandraConnector.openSession()) {
			setupData(sparkCtx, session);
			return trainAndValidate(sparkCtx, RANK, ITER);
		}
	}

	private static void setupData(JavaSparkContext sparkCtx, Session cassSession) {
		truncateTables(cassSession);
		loadData(sparkCtx);
	}

	private static void truncateTables(Session cassSession) {
		cassSession.execute("TRUNCATE " + EMPLOYERRATINGS_KEYSPACE + "." + RATINGS_TABLE);
		cassSession.execute("TRUNCATE " + EMPLOYERRATINGS_KEYSPACE + "." + VALIDATION_TABLE);
	}

	private static void loadData(JavaSparkContext sparkCtx) {
		org.apache.spark.api.java.function.Function<String, RatingDO> ratingsBuilder = new org.apache.spark.api.java.function.Function<String, RatingDO>() {
			@Override
			public RatingDO call(String line) throws Exception {
				String[] fields = line.split(",");
				return new RatingDO(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Double.parseDouble(fields[3]));
			}
		};
		saveToCassandra(RATINGS_TABLE, loadCsv(INPUT_SET, sparkCtx, ratingsBuilder));
		saveToCassandra(VALIDATION_TABLE, loadCsv(VALIDATION_SET, sparkCtx, ratingsBuilder));
	}

	private static JavaRDD<RatingDO> loadCsv(final String prefix, JavaSparkContext sc, org.apache.spark.api.java.function.Function<String, RatingDO> ratingsBuilder) {
		return sc.textFile(RATINGS_CSV).filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
			@Override
			public Boolean call(String line) throws Exception {
				return line.startsWith(prefix);
			}
		}).map(ratingsBuilder);
	}

	private static void saveToCassandra(String table, JavaRDD<RatingDO> rdd) {
		RDDAndDStreamCommonJavaFunctions<RatingDO>.WriterBuilder writerBuilder = CassandraJavaUtil.javaFunctions(rdd).writerBuilder(EMPLOYERRATINGS_KEYSPACE, table, CassandraJavaUtil.mapToRow(RatingDO.class));
		writerBuilder.saveToCassandra();
	}

	private static MatrixFactorizationModel trainAndValidate(JavaSparkContext sc, int rank, int iterations) {
		MatrixFactorizationModel model = train(sc);
		predictAndValidate(sc, model);
		return model;
	}

	private static MatrixFactorizationModel train(JavaSparkContext sc) {
		CassandraJavaRDD<CassandraRow> trainingRdd = javaFunctions(sc).cassandraTable(EMPLOYERRATINGS_KEYSPACE, RATINGS_TABLE);
		JavaRDD<Rating> trainingJavaRdd = trainingRdd.map(new org.apache.spark.api.java.function.Function<CassandraRow, Rating>() {
			@Override
			public Rating call(CassandraRow trainingRow) throws Exception {
				return new Rating(trainingRow.getInt(USER_COL), trainingRow.getInt(PRODUCT_COL), trainingRow.getDouble(RATING_COL));
			}
		});
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingJavaRdd), RANK, ITER, LAMBDA);
		return model;
	}

	private static void predictAndValidate(JavaSparkContext sc, MatrixFactorizationModel model) {
		CassandraJavaRDD<CassandraRow> validationsCassRdd = javaFunctions(sc).cassandraTable(EMPLOYERRATINGS_KEYSPACE, VALIDATION_TABLE);
		final RDD<Tuple2<Object, Object>> validationsRdd = JavaRDD.toRDD(validationsCassRdd.map(new org.apache.spark.api.java.function.Function<CassandraRow, Tuple2<Object, Object>>() {

			@Override
			public Tuple2<Object, Object> call(CassandraRow validationRow) throws Exception {
				return new Tuple2<Object, Object>(validationRow.getInt(USER_COL), validationRow.getInt(PRODUCT_COL));
			}
		}));
		JavaRDD<Rating> predictionJavaRdd = model.predict(validationsRdd).toJavaRDD();
		Double rmse = rootMeanSquaredError(predictionJavaRdd, validationsCassRdd);
		showResults(predictionJavaRdd, validationsCassRdd, rmse);

	}

	private static double rootMeanSquaredError(JavaRDD<Rating> predictionsJavaRDD, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictionsJavaPairs = JavaPairRDD.fromJavaRDD(predictionsJavaRDD.map(new org.apache.spark.api.java.function.Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating pred) throws Exception {
				return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(pred.user(), pred.product()), pred.rating());
			}
			//
		}));
		JavaRDD<Rating> validationRatings = validationsCassRdd.map(new org.apache.spark.api.java.function.Function<CassandraRow, Rating>() {
			@Override
			public Rating call(CassandraRow validation) throws Exception {
				return new Rating(validation.getInt(USER_COL), validation.getInt(PRODUCT_COL), validation.getInt(RATING_COL));
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
											// toObjec
			}
		}).rdd()).mean();
		double rmse = Math.sqrt(meanSquaredError);
		return rmse;
	}

	private static void showResults(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd, double rmse) {
		final String resultStr = "User\tProduct\tPredicted\tActual\tError?\n" + predictionString(predJavaRdd, validationsCassRdd) + "\n" + "RMSE = " + round(rmse, 2);
		System.out.println(resultStr);
	}

	private static String predictionString(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		final java.util.function.Function<CassandraRow, Tuple2<Integer, Integer>> keyMapper = new java.util.function.Function<CassandraRow, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> apply(CassandraRow validationRow) {
				return new Tuple2<Integer, Integer>(validationRow.getInt(USER_COL), validationRow.getInt(PRODUCT_COL));
			}
		};
		final java.util.function.Function<CassandraRow, Double> valueMapper = new java.util.function.Function<CassandraRow, Double>() {
			@Override
			public Double apply(CassandraRow validationRow) {
				return validationRow.getDouble(RATING_COL);
			}

		};

		final java.util.Map<Tuple2<Integer, Integer>, Double> validationMap = validationsCassRdd.collect().stream().collect(Collectors.toMap(keyMapper, valueMapper));

		final java.util.function.Function<Rating, String> stringMapper = new java.util.function.Function<Rating, String>() {
			@Override
			public String apply(Rating prediction) {
				double validationRating = validationMap.get(new Tuple2<Integer, Integer>(prediction.user(), prediction.product()));
				String errWarningString = Math.abs(validationRating - prediction.rating()) >= 1 ? "ERR" : "OK";
				return prediction.user() + "\t" + prediction.product() + "\t" + round(prediction.rating()) + "\t\t" + round(validationRating) + "\t" + errWarningString;
			}
		};

		final Stream<Rating> sortedPredictions = predJavaRdd.collect().stream().sorted(new Comparator<Rating>() {
			public int compare(Rating o1, Rating o2) {
				return o1.user() == o2.user() ? o1.product() - o2.product() : o1.user() - o2.user();
			}
		});
		final String ret = sortedPredictions.map(stringMapper).collect(Collectors.joining("\n"));

		return ret;
	}

	static double round(double x) {
		return round(x, 1);
	}

	static double round(double x, int places) {
		double factor = Math.pow(10, places);
		return Math.round(factor * x) / factor;
	}
}