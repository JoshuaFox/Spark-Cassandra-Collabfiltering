package collabfilter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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

import scala.Tuple2;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

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

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering with Cassandra").set("spark.master", SPARK_MASTER).set("spark.cassandra.connection.host", CASSANDRA_HOST);
		JavaSparkContext sparkCtx = new JavaSparkContext(conf);
		CassandraConnector cassandraConnector = CassandraConnector.apply(sparkCtx.getConf());

		try (Session session = cassandraConnector.openSession()) {
			setupData(sparkCtx, session);
			for (int rank=2;rank<=5;rank++){
				for (int iter=10;iter<=30;iter+=10){
					trainAndValidate(sparkCtx, rank, iter);
				}
			}
		}
		mseList.sort((tuple1, tuple2)->tuple1._1().compareTo(tuple2._1()));
		System.out.println(mseList);
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
		org.apache.spark.api.java.function.Function<String, RatingDO> ratingsBuilder = line -> {
			String[] fields = line.split(",");
			return new RatingDO(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Double.parseDouble(fields[3]));
		};
		save(RATINGS_TABLE, loadCsv(INPUT_SET, sparkCtx, ratingsBuilder));
		save(VALIDATION_TABLE, loadCsv(VALIDATION_SET, sparkCtx, ratingsBuilder));
	}

	private static JavaRDD<RatingDO> loadCsv(String prefix, JavaSparkContext sc, org.apache.spark.api.java.function.Function<String, RatingDO> ratingsBuilder) {
		return sc.textFile(RATINGS_CSV).filter(line -> line.startsWith(prefix)).map(ratingsBuilder);
	}

	private static void save(String table, JavaRDD<RatingDO> rdd) {
		RDDAndDStreamCommonJavaFunctions<RatingDO>.WriterBuilder writerBuilder = CassandraJavaUtil.javaFunctions(rdd).writerBuilder(EMPLOYERRATINGS_KEYSPACE, table, CassandraJavaUtil.mapToRow(RatingDO.class));
		writerBuilder.saveToCassandra();
	}

	private static void trainAndValidate(JavaSparkContext sc, int rank, int iterations) {
		CassandraJavaRDD<CassandraRow> trainingRdd = javaFunctions(sc).cassandraTable(EMPLOYERRATINGS_KEYSPACE, RATINGS_TABLE);
		JavaRDD<Rating> trainingJavaRdd = trainingRdd.map(trainingRow -> new Rating(trainingRow.getInt(USER_COL), trainingRow.getInt(PRODUCT_COL), trainingRow.getDouble(RATING_COL)));
	 
		final String s = "RANK "+rank+"\tITER "+iterations+"\t";
		System.out.print(s);
	 
		final double lambda = 0.01;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingJavaRdd), rank, iterations, lambda);
		predictAndValidate(sc, model, trainingJavaRdd, s);
	}

	private static void predictAndValidate(JavaSparkContext sc, MatrixFactorizationModel model, JavaRDD<Rating> trainingJavaRdd, String sss) {
		CassandraJavaRDD<CassandraRow> validationsCassRdd = javaFunctions(sc).cassandraTable(EMPLOYERRATINGS_KEYSPACE, VALIDATION_TABLE);
		JavaRDD<Tuple2<Object, Object>> validationJavaTuplesRdd = validationsCassRdd.map(validationRow -> new Tuple2<Object, Object>(validationRow.getInt(USER_COL), validationRow.getInt(PRODUCT_COL)));
		JavaRDD<Rating> predictionJavaRdd = model.predict(JavaRDD.toRDD(validationJavaTuplesRdd)).toJavaRDD();
		validate(predictionJavaRdd, validationsCassRdd, sss);
	}

	private static double calculateMeanSquaredError(JavaRDD<Rating> predictionsJavaRDD, CassandraJavaRDD<CassandraRow> validationsCassRdd) {
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictionsJavaPairs = JavaPairRDD.fromJavaRDD(predictionsJavaRDD.map(pred -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(pred.user(), pred.product()), pred.rating())));
		JavaRDD<Rating> validationRatings = validationsCassRdd.map(validation -> new Rating(validation.getInt(USER_COL), validation.getInt(PRODUCT_COL), validation.getInt(RATING_COL)));
		JavaRDD<Tuple2<Double, Double>> validationAndPredictions = JavaPairRDD.fromJavaRDD(validationRatings.map(validationRating -> new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(validationRating.user(), validationRating.product()), validationRating.rating()))).join(predictionsJavaPairs).values();

		double meanSquaredError = JavaDoubleRDD.fromRDD(validationAndPredictions.map(pair -> {
			Double err = pair._1() - pair._2();
			return (Object) (err * err);// No covariance! Double won't do
			}).rdd()).mean();

		return meanSquaredError;
	}

	private static void validate(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd, String s) {
		Stream<CassandraRow> stream = validationsCassRdd.collect().stream();

		java.util.function.Function<CassandraRow, Tuple2<Integer, Integer>> keyMapper = validationRow -> new Tuple2<Integer, Integer>(validationRow.getInt(USER_COL), validationRow.getInt(PRODUCT_COL));
		java.util.function.Function<CassandraRow, Double> valueMapper = validationRow -> validationRow.getDouble(RATING_COL);
		java.util.Map<Tuple2<Integer, Integer>, Double> validationMap = stream.collect(Collectors.toMap(keyMapper, valueMapper));
		List<String> strList = predJavaRdd.collect().stream().sequential().sorted((o1, o2) -> o1.user() == o2.user() ? o1.product() - o2.product() : o1.user() - o2.user()).map(pred -> {
			double validationRating = validationMap.get(new Tuple2<Integer, Integer>(pred.user(), pred.product()));
			String errWarningString = Math.abs(validationRating - pred.rating()) > 1 ? "\t!!!" : "";

			return "\n" + pred.user() + "\t" + pred.product() + "\t" + round(pred.rating()) + "\t\t" + round(validationRating) + errWarningString;
		}).collect(Collectors.toList());

		Double mse = calculateMeanSquaredError(predJavaRdd, validationsCassRdd);
		//String s = "User\tProduct\tPredicted\tActual\tError?" + strList + "\n";
		String s_ ="MSE=" + round(mse,2);
		System.out.println(s+s_);
		final Tuple2<Double, String> tuple2 = new Tuple2<Double, String>(Double.valueOf(mse),s);
		mseList.add(tuple2);
		
		//System.out.println();
	}
static ArrayList<Tuple2<Double, String>> mseList=new ArrayList<Tuple2<Double,String>>();
	private static double round(double x) {
		return round(x, 1);
	}

	private static double round(double x, int places) {
		double factor = Math.pow(10, places);
		return Math.round(factor * x) / factor;
	}

	public static class RatingDO {
		private UUID id;
		private int product;
		private int user;
		private double rating;

		public RatingDO(UUID id, int user, int product, double rating) {
			this.id = id;
			this.user = user;
			this.product = product;
			this.rating = rating;
		}

		public RatingDO(int user, int product, double rating) {
			this(UUIDs.timeBased(), user, product, rating);
		}

		public UUID getId() {
			return this.id;
		}

		public int getUser() {
			return this.user;
		}

		public double getProduct() {
			return this.product;
		}

		public double getRating() {
			return this.rating;
		}
	}
}
