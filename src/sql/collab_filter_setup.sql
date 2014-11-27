--set up database
DROP KEYSPACE IF EXISTS employerratings;
CREATE KEYSPACE employerratings WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE employerratings.ratings (id TIMEUUID PRIMARY KEY, user INT, product INT, rating DOUBLE);
CREATE TABLE employerratings.validation (id	 TIMEUUID PRIMARY KEY, user INT, product INT, rating DOUBLE);
