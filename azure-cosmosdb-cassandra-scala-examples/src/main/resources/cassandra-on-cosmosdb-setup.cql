CREATE KEYSPACE IF NOT EXISTS scalatest  WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 } ;

USE scalatest;

CREATE TABLE IF NOT EXISTS order_item (
    id int,
    shop text,
    item text,
    coupon_used boolean,
    customer_name text,
    mobile text,
    price double,
    quantity int,
    PRIMARY KEY ((id, shop), item)
) WITH CLUSTERING ORDER BY (item ASC);


CREATE TABLE IF NOT EXISTS cache_data (
	key text,
	value text,	
	PRIMARY KEY(key)
)WITH default_time_to_live = 10;