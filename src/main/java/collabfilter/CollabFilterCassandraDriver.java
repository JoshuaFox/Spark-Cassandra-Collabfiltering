package collabfilter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.Closeable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

/**
 * Launch the implementations which are coded in Java 7 and Java 8.
 */
public class CollabFilterCassandraDriver implements Closeable {

	private static final String SPARK_MASTER = "local[4]";
	private static final String CASSANDRA_HOST = "localhost";

	private static final String RATINGS_CSV = "data/csv/ratings.csv";

	private CassandraConnector cassandraConnector;
	private JavaSparkContext sparkCtx;

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		final String versionStr = args[0];
		if (!versionStr.equals("7") && !versionStr.equals("8")) {
			System.err.println("Usage: java collabfilter.CollabFilterCassandraDriver [7|8]");
			System.exit(1);
		}

		int version = args.length < 1 ? 8 : Integer.parseInt(versionStr);
		System.out.printf("Java %d\n", version);

		try (CollabFilterCassandraDriver cfcDriver = new CollabFilterCassandraDriver()) {
			cfcDriver.truncateTables();
			cfcDriver.populateTables();
			cfcDriver.trainAndValidate(version);
		}
	}

	public CollabFilterCassandraDriver() {
		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering with Cassandra").set("spark.master", SPARK_MASTER).set("spark.cassandra.connection.host", CASSANDRA_HOST);
		this.sparkCtx = new JavaSparkContext(conf);
		this.cassandraConnector = CassandraConnector.apply(this.sparkCtx.getConf());
	}

	double trainAndValidate(int version) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		final ICollabFilterCassandra cfc;
		String className = "collabfilter.CollabFilterCassandra" + version;
		cfc = (ICollabFilterCassandra) Class.forName(className).newInstance();
		try (Session session = this.cassandraConnector.openSession()) {
			MatrixFactorizationModel model = cfc.train(this.sparkCtx, this.cassandraConnector);
			CassandraJavaRDD<CassandraRow> validationsCassRdd = javaFunctions(this.sparkCtx).cassandraTable(RatingDO.EMPLOYERRATINGS_KEYSPACE, RatingDO.VALIDATION_TABLE);
			JavaRDD<Rating> predictionJavaRdd = cfc.predict(model, validationsCassRdd);
			double rmse = cfc.validate(predictionJavaRdd, validationsCassRdd);
			System.out.println(cfc.resultsReport(predictionJavaRdd, validationsCassRdd, rmse));
			return rmse;
		}

	}

	public void truncateTables() {
		try (Session session = this.cassandraConnector.openSession()) {
			session.execute("TRUNCATE " + RatingDO.EMPLOYERRATINGS_KEYSPACE + "." + RatingDO.RATINGS_TABLE);
			session.execute("TRUNCATE " + RatingDO.EMPLOYERRATINGS_KEYSPACE + "." + RatingDO.VALIDATION_TABLE);
		}
	}

	/*
	 * For use with try-with-resources.
	 */
	@Override
	public void close() {
		truncateTables();
	}

	public void populateTables() {
		try (Session session = this.cassandraConnector.openSession()) {
			org.apache.spark.api.java.function.Function<String, RatingDO> ratingsBuilder = new RatingsBuilder();
			saveToCassandra(loadCsvtoTable(RatingDO.INPUT_SET, ratingsBuilder), RatingDO.RATINGS_TABLE);
			saveToCassandra(loadCsvtoTable(RatingDO.VALIDATION_SET, ratingsBuilder), RatingDO.VALIDATION_TABLE);
		}
	}

	private JavaRDD<RatingDO> loadCsvtoTable(final String prefix, org.apache.spark.api.java.function.Function<String, RatingDO> ratingsBuilder) {
		final JavaRDD<String> textFile = this.sparkCtx.textFile(RATINGS_CSV);
		final JavaRDD<RatingDO> map = textFile.filter(new FilterCsv(prefix)).map(ratingsBuilder);
		return map;
	}

	private void saveToCassandra(JavaRDD<RatingDO> rdd, String table) {
		RDDAndDStreamCommonJavaFunctions<RatingDO>.WriterBuilder writerBuilder = CassandraJavaUtil.javaFunctions(rdd).writerBuilder(RatingDO.EMPLOYERRATINGS_KEYSPACE, table, CassandraJavaUtil.mapToRow(RatingDO.class));
		writerBuilder.saveToCassandra();
	}

	// These classes declared static rather than defined local-anonymous to
	// avoid including the outer class, which would try to serialize the
	// unserializable SparkContext. They are written in Java-7-compatible style
	// because
	// this driver must access both the Java 7 and Java 8 versions.
	private static final class FilterCsv implements org.apache.spark.api.java.function.Function<String, Boolean> {
		private final String prefix;

		FilterCsv(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public Boolean call(String line) throws Exception {
			return line.startsWith(this.prefix);
		}
	}

	private static final class RatingsBuilder implements org.apache.spark.api.java.function.Function<String, RatingDO> {
		// Explicit constructor needed to avoid compiler warning about a
		// synthsized constructor.
		RatingsBuilder() {
		}

		@Override
		public RatingDO call(String line) throws Exception {
			String[] fields = line.split(",");
			return new RatingDO(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Double.parseDouble(fields[3]));
		}
	}

}