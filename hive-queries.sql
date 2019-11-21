CREATE DATABASE IF NOT EXISTS sales;

CREATE EXTERNAL TABLE IF NOT EXISTS sales.purchases (name STRING, price DOUBLE, timestampDate STRING, category STRING, ipAddress STRING) PARTITIONED BY (date STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/flume/events';
alter table sales.purchases add partition(date='2019-08-31') location '/tmp/flume/events/19/08/31';
alter table sales.purchases add partition(date='2019-09-01') location '/tmp/flume/events/19/09/01';
alter table sales.purchases add partition(date='2019-09-02') location '/tmp/flume/events/19/09/02';
alter table sales.purchases add partition(date='2019-09-03') location '/tmp/flume/events/19/09/03';
alter table sales.purchases add partition(date='2019-09-04') location '/tmp/flume/events/19/09/04';
alter table sales.purchases add partition(date='2019-09-05') location '/tmp/flume/events/19/09/05';
alter table sales.purchases add partition(date='2019-09-06') location '/tmp/flume/events/19/09/06';


-- Select top 10  most frequently purchased categories
	select category, count(*) num from sales.purchases GROUP BY category ORDER BY num desc LIMIT 10;


-- Select top 10 most frequently purchased product in each category
	CREATE TABLE IF NOT EXISTS sales.purchases_analytics (name STRING, category STRING, salesnumber INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
	insert overwrite table sales.purchases_analytics select p.name, p.category, count(*) sNum from purchases p group by p.name, p.category order by p.category, sNum desc;
	select name, category, salesnumber, rank from (select name, category, salesnumber, rank() over (partition by category order by salesnumber desc) rank from purchases_analytics) X where rank <= 10;


-- Countries && IPv4
	CREATE EXTERNAL TABLE IF NOT EXISTS sales.countries (geoname_id BIGINT, locale_code STRING, continent_code STRING, continent_name STRING, country_iso_code STRING, country_name STRING, is_in_european_union INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/countries' tblproperties ("skip.header.line.count"="1");
	CREATE EXTERNAL TABLE IF NOT EXISTS sales.ips (network STRING, geoname_id BIGINT, registered_country_geoname_id BIGINT, represented_country_geoname_id BIGINT, is_anonymous_proxy INT, is_satellite_provider INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/ips' tblproperties ("skip.header.line.count"="1");
	CREATE TABLE IF NOT EXISTS sales.countries_ips_analytics (network STRING, geoname_id BIGINT, locale_code STRING, continent_name STRING, country_name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
	insert overwrite table sales.countries_ips_analytics select split(ip.network,'\\/')[0], c.geoname_id, c.locale_code, c.continent_name, c.country_name from countries c JOIN ips ip ON (c.geoname_id = ip.geoname_id) where c.country_name != '';

-- Countries && purchases
	CREATE TABLE IF NOT EXISTS sales.countries_purchases_analytics (name STRING, price DOUBLE, category STRING, network STRING, country_name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
	insert overwrite table sales.countries_purchases_analytics select p.name, p.price, p.category, p.ipAddress, c.country_name from purchases p JOIN countries_ips_analytics c ON (p.ipAddress = c.network);
	select country_name, sum(price) as money from sales.countries_purchases_analytics group by country_name order by money desc limit 10;


-- Exporting via Sqoop:
-- 	top 10 categories:
			CREATE TABLE IF NOT EXISTS sales.top_categories (category STRING, sales_number INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
			insert overwrite table sales.top_categories select p.category, count(*) num from purchases p GROUP BY p.category ORDER BY num desc LIMIT 10;
			sqoop export --connect jdbc:mysql://quickstart.cloudera/test --driver com.mysql.jdbc.Driver --username root --password cloudera --table test.top_categories --hcatalog-database sales --hcatalog-table top_categories --fields-terminated-by ',';

-- 	top 10 products in each category
			CREATE TABLE IF NOT EXISTS sales.top_products_in_categories (name STRING, category STRING, sales_number INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
			insert overwrite table sales.top_products_in_categories select name, category, salesnumber from (select name, category, salesnumber, rank() over (partition by category order by salesnumber desc) rank from purchases_analytics) X where rank <= 10;
			sqoop export --connect jdbc:mysql://quickstart.cloudera/test --driver com.mysql.jdbc.Driver --username root --password cloudera --table test.top_products_in_categories --hcatalog-database sales --hcatalog-table top_products_in_categories --fields-terminated-by ',';

-- 	top 10 countries by money spending
			CREATE TABLE IF NOT EXISTS sales.top_countries_by_spending (name STRING, spending DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
			insert overwrite table sales.top_countries_by_spending select country_name, sum(price) as money from sales.countries_purchases_analytics group by country_name order by money desc limit 10;
			sqoop export --connect jdbc:mysql://quickstart.cloudera/test --driver com.mysql.jdbc.Driver --username root --password cloudera --table test.top_countries_by_spending --hcatalog-database sales --hcatalog-table top_countries_by_spending --fields-terminated-by ',';