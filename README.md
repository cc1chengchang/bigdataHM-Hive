
建库和建表
create database if not exists chchang;
use chchang;

CREATE EXTERNAL TABLE `t_movie`(
  `movieid` int COMMENT 'from deserializer',
  `moviename` string COMMENT 'from deserializer',
  `movietype` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/movies'
TBLPROPERTIES (
  'bucketing_version'='2',
  'transient_lastDdlTime'='1648805564')


CREATE EXTERNAL TABLE `t_rating`(
  `userid` int COMMENT 'from deserializer',
  `movieid` int COMMENT 'from deserializer',
  `rate` int COMMENT 'from deserializer',
  `times` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/ratings'
TBLPROPERTIES (
  'bucketing_version'='2',
  'transient_lastDdlTime'='1648805728')


CREATE EXTERNAL TABLE `t_user`(
  `userid` int COMMENT 'from deserializer',
  `sex` string COMMENT 'from deserializer',
  `age` int COMMENT 'from deserializer',
  `occupation` int COMMENT 'from deserializer',
  `zipcode` int COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-285604:9000/data/hive/users'
TBLPROPERTIES (
  'bucketing_version'='2',
  'transient_lastDdlTime'='1648805292')


题目1答案

With t_user_rating as ( 
SELECT t_user.age, t_rating.rate FROM t_rating JOIN t_user on (t_rating.userid == t_user.userid) WHERE t_rating.movieid==2116) 
SELECT t_user_rating.age AS age, avg(t_user_rating.rate) as avgrate FROM t_user_rating GROUP BY t_user_rating.age ORDER BY t_user_rating.age;

OK
1	3.2941176470588234
18	3.3580246913580245
25	3.436548223350254
35	3.2278481012658227
45	2.8275862068965516
50	3.32
56	3.5
Time taken: 18.199 seconds, Fetched: 7 row(s)
hive>


题目2答案

SELECT t_user.sex as sex,  t_movie.moviename as name, avg(t_rating.rate) as avgrate, count(*) as total
FROM t_rating
JOIN t_user on (t_rating.userid == t_user.userid and t_user.sex == 'M')
JOIN t_movie on (t_rating.movieid == t_movie.movieid)
GROUP BY t_movie.movieid, t_user.sex, t_movie.moviename
HAVING count(*) > 50
ORDER BY avgrate DESC
limit 10;

OK
M	Sanjuro (1962)	4.639344262295082	61
M	Godfather, The (1972)	4.583333333333333	1740
M	Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)	4.576628352490421	522
M	Shawshank Redemption, The (1994)	4.560625	1600
M	Raiders of the Lost Ark (1981)	4.520597322348094	1942
M	Usual Suspects, The (1995)	4.518248175182482	1370
M	Star Wars: Episode IV - A New Hope (1977)	4.495307167235495	2344
M	Schindler's List (1993)	4.49141503848431	1689
M	Paths of Glory (1957)	4.485148514851486	202
M	Wrong Trousers, The (1993)	4.478260869565218	644
Time taken: 20.319 seconds, Fetched: 10 row(s)
hive>

