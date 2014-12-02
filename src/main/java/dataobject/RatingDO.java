package dataobject;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class RatingDO {
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