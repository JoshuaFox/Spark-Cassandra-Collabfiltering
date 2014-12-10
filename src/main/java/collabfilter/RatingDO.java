package collabfilter;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class RatingDO {
	public static final String EMPLOYERRATINGS_KEYSPACE = "employerratings";
	public static final String RATINGS_TABLE = "ratings";
	public static final String VALIDATION_TABLE = "validation";
	public static final String USER_COL = "user";
	public static final String PRODUCT_COL = "product";
	public static final String RATING_COL = "rating";
	static final String INPUT_SET = "I,";
	static final String VALIDATION_SET = "V,";
	
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