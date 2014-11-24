package collabfilter;

import java.io.Serializable;
import java.text.MessageFormat;
 

public class ProductRating implements Serializable {

	private Integer id;
	private Double rating;
	private Integer itemId;
	private Integer userId;

	public ProductRating() {
	}

	public ProductRating(Integer id, Double rating_, Integer itemId_, Integer userId_) {
		this.id = id;
		this.rating = rating_;
		this.itemId = itemId_;
		this.userId = userId_;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Double getRating() {
		return rating;
	}

	public void setRating(Double rating_) {
		this.rating = rating_;
	}

	public Integer getitemId() {
		return itemId;
	}

	public void setItemId(int itemId) {
		this.itemId = itemId;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId_) {
		this.userId = userId_;
	}

	@Override
	public String toString() {
		return MessageFormat.format("Rating id={0}, user={1}, product={2}, rating={rating}", id, userId, itemId, rating);
	}

}