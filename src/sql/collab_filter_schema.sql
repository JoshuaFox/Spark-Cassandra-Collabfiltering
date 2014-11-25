DROP KEYSPACE IF EXISTS employerratings;
CREATE KEYSPACE employerratings WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE employerratings.ratings (rating_id INT PRIMARY KEY, user_id INT, item_id INT, rating DOUBLE);
 