package collabfilter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public interface ICollabFilterCassandra {


	MatrixFactorizationModel train(JavaSparkContext sparkCtx, CassandraConnector cassandraConnector);

	JavaRDD<Rating> predict(MatrixFactorizationModel model, CassandraJavaRDD<CassandraRow> validationsCassRdd);

	double calculateRootMeanSquareError(JavaRDD<Rating> predictionJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd);

	public abstract String resultsReport(JavaRDD<Rating> predJavaRdd, CassandraJavaRDD<CassandraRow> validationsCassRdd, double rmse);
}
