package collabfilter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hadoop.mapred.loadhistory_jsp;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import tachyon.thrift.MasterService.Processor.liststatus;

import com.clearspring.analytics.util.Pair;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.writer.RowWriterFactory;

public class CollabFilterCassandra {
	private static final String LOCALHOST = "localhost";
	private static final String LOCAL_MASTER = "local[4]";

	private static final String EMPLOYERRATINGS_KEYSPACE = "employerratings";
	private static final String RATINGS_TABLE = "ratings";
	private static final String VALIDATION_TABLE = "validation";

	private static final String USER_ID_COL = "user_id";
	private static final String ITEM_ID_COL = "item_id";
	private static final String RATING_COL = "rating";

	private static final String SPARK_MASTER = LOCAL_MASTER;
	private static final String CASSANDRA_HOST = LOCALHOST;
	private static final String RATINGS_CSV = "data/csv/ratings.csv";
 

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering with Cassandra").set("spark.master", SPARK_MASTER).set("spark.cassandra.connection.host", CASSANDRA_HOST);

		JavaSparkContext sc = new JavaSparkContext(conf);
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		try (Session session = connector.openSession()) {
			loadData(sc);
			trainAndValidate(sc);

		}
	}

	private static void loadData(JavaSparkContext sc) {
 
		org.apache.spark.api.java.function.Function<String, Rating> ratingsBuilder = line-> {
	 				String[] fields = line.split(",");
					int usr = Integer.parseInt(fields[1]);
					int item = Integer.parseInt(fields[2]);
					Double rating = Double.parseDouble(fields[3]);
					return new Rating(usr, item, rating);
				};
		save(RATINGS_TABLE, loadCsv(sc, ratingsBuilder, "I,"));
		save(VALIDATION_TABLE, loadCsv(sc, ratingsBuilder, "V,"));
 
	}

	private static JavaRDD<Rating> loadCsv(JavaSparkContext sc, org.apache.spark.api.java.function.Function<String, Rating> ratingsBuilder, String pfx) {
		JavaRDD<Rating> rddInput = sc.textFile(RATINGS_CSV).filter(line->line.startsWith(pfx)).map(ratingsBuilder);
		return rddInput;
	}

	private static void save(String table, JavaRDD<Rating> rddInput) {
		CassandraJavaUtil.javaFunctions(rddInput).writerBuilder(EMPLOYERRATINGS_KEYSPACE,table,CassandraJavaUtil.mapToRow(Rating.class)).saveToCassandra();
	}

	private static void trainAndValidate(JavaSparkContext sc) {
		MatrixFactorizationModel model = train(sc);
		compareToValidationSet(sc, model);
	}

	private static void compareToValidationSet(JavaSparkContext sc, MatrixFactorizationModel model) {
		CassandraJavaRDD<CassandraRow> validationsCassRdd = javaFunctions(sc).cassandraTable(EMPLOYERRATINGS_KEYSPACE, VALIDATION_TABLE);
		JavaRDD<Tuple2<Object, Object>> validationJavaRdd = validationsCassRdd.map(validationRow -> new Tuple2<Object, Object>(validationRow.getInt(USER_ID_COL), validationRow.getInt(ITEM_ID_COL)));

		JavaRDD<Rating> predictionJavaRatingsRDD = model.predict(JavaRDD.toRDD(validationJavaRdd)).toJavaRDD();
		Map<Tuple2<Integer, Integer>, Double> validationMap = validationsCassRdd
				.collect()
				.stream()
				.collect(
						Collectors.toMap(validationRow -> new Tuple2<Integer, Integer>(validationRow.getInt(USER_ID_COL), validationRow.getInt(ITEM_ID_COL)),
								validationRow -> validationRow.getDouble(RATING_COL)));

		List<Rating> list = predictionJavaRatingsRDD.collect();

		for (Rating pred : list) {
			int user = pred.user();
			int product = pred.product();
			double validationRating = validationMap.get(new Tuple2<Integer, Integer>(user, product));
			System.out.println("user " + user + " and item " + product + "; predicted rating " + round(pred.rating(), 1) + "; actual rating " + round(validationRating, 1));

		}
	}

	private static MatrixFactorizationModel train(JavaSparkContext sc) {
		CassandraJavaRDD<CassandraRow> trainingRdd = javaFunctions(sc).cassandraTable(EMPLOYERRATINGS_KEYSPACE, RATINGS_TABLE);
		JavaRDD<Rating> trainingRatingsRdd = trainingRdd.map(trainingRow -> new Rating(trainingRow.getInt(USER_ID_COL), trainingRow.getInt(ITEM_ID_COL), trainingRow.getDouble(RATING_COL)));

		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingRatingsRdd), 10, 30);
		return model;
	}

	private static double round(double x, int places) {
		double factor = Math.pow(10, places);
		return Math.round(factor * x) / factor;
	}

}