--set up database
DROP KEYSPACE IF EXISTS employerratings;
CREATE KEYSPACE employerratings WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE employerratings.ratings (rating_id TIMEUUID PRIMARY KEY, user_id INT, item_id INT, rating DOUBLE);
CREATE TABLE employerratings.validation (rating_id TIMEUUID PRIMARY KEY, user_id INT, item_id INT, rating DOUBLE);

use employerratings;

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 1, 1, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 1, 2, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 1, 3, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 1, 4, 10);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 2, 1, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 2, 2, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 2, 3, 10);
insert into validation (rating_id, user_id, item_id, rating) values(now(), 2, 4, 1);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 3, 1, 1);
insert into validation (rating_id, user_id, item_id, rating) values(now(), 3, 2, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 3, 3, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 3, 4, 10);						

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 4, 1, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 4, 2, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 4, 3, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 4, 4, 1);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 5, 1, 1);
insert into validation (rating_id, user_id, item_id, rating) values(now(), 5, 2, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 5, 3, 1);	
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 5, 4, 10);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 6, 1, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 6, 2, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 6, 3, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 6, 4, 1);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 7, 1, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 7, 2, 10);
insert into validation (rating_id, user_id, item_id, rating) values(now(), 7, 3, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 7, 4, 10);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 8, 1, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 8, 2, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 8, 3, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 8, 4, 1);

insert into ratings (rating_id, user_id, item_id, rating) values(now(), 9, 1, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 9, 2, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 9, 3, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 9, 4, 10);

insert into validation (rating_id, user_id, item_id, rating) values(now(), 10, 1, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 10, 2, 1);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 10, 3, 10);
insert into ratings (rating_id, user_id, item_id, rating) values(now(), 10, 4, 1);

