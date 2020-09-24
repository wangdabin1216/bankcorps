DROP TABLE tbo.hs_test1
CREATE EXTERNAL  TABLE tbo.hs_test1(
  begndt string , 
  overdt string , 
  custno STRING,
  bal decimal(10,2)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'input.delimited'='~@~', 
  'serialization.format'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/dw/tmp/hs_test1.txt'


CREATE EXTERNAL  TABLE tbo.hs_test2(
  begndt string , 
  overdt string , 
  custno STRING,
  bal decimal(10,2)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ( 
  'input.delimited'='~@~', 
  'serialization.format'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/dw/tmp/hs_test2.txt'

hdfs dfs -put ./hs_test1.txx /dw/tmp/hs_test1.txt
hdfs dfs -put ./hs_test2.txx /dw/tmp/hs_test2.txt

hdfs dfs -rm /dw/tmp/hs_test1.txt/hs_test1.txx 
hdfs dfs -rm /dw/tmp/hs_test2.txt/hs_test2.txx 

SELECT *FROM tbo.hs_test1
SELECT *FROM tbo.hs_test2

--取有效数据
SELECT * FROM tbo.hs_test1 WHERE overdt='20991231'
--取某一天数据
SELECT * FROM tbo.hs_test1 WHERE begndt <='20200315' AND overdt>'20200315'
SELECT * FROM tbo.hs_test1 WHERE begndt <='20200314' AND overdt>'20200314'
--取当天新增的数据
SELECT * FROM tbo.hs_test1 WHERE begndt <='20200315' AND overdt>'20200315'
and custno NOT IN (SELECT custno FROM tbo.hs_test1 WHERE begndt <='20200314' AND overdt>'20200314' )
--取删除的数据
SELECT * FROM tbo.hs_test1 WHERE begndt <='20200314' AND overdt>'20200314'
and custno NOT IN (SELECT custno FROM tbo.hs_test1 WHERE begndt <='20200315' AND overdt>'20200315' )
--取修改的数据
SELECT * FROM tbo.hs_test1 WHERE begndt <='20200315' AND overdt>'20200315'
and custno IN (SELECT custno FROM tbo.hs_test1 WHERE begndt <='20200314' AND overdt>'20200314' )
--取一段时间的数据
SELECT *FROM tbo.hs_test1 WHERE begndt <='20200315' AND overdt>'20200301' AND custno='1001'
SELECT *FROM tbo.hs_test2 WHERE begndt <='20200315' AND overdt>'20200301' AND custno='1001'
--计算一段时间的基数
--计算一段时间的基数
--计算一段时间的基数
SELECT custno,sum(
CASE WHEN begndt <='20200301' AND overdt>'20200315' THEN bal* datediff(DATE'20200315',DATE'20200301')
WHEN begndt <='20200301' AND overdt>'20200301' AND overdt<='20200315' THEN bal* datediff(date(overdt),DATE'20200301')
WHEN begndt >'20200301' AND begndt<='20200315' AND overdt>'20200315' THEN bal* (datediff(DATE'20200315',date(begndt)) +1)
WHEN begndt >'20200301' AND begndt<='20200315' AND overdt<='20200315' THEN bal* datediff(date(overdt),date(begndt))
ELSE 0 END
)
FROM tbo.hs_test1 
WHERE begndt <='20200315' AND overdt>'20200301' 
--AND custno='1001'
GROUP BY custno

1001	1700
1002	200
1003	500
SELECT custno,sum(
CASE WHEN begndt <='20200301' AND overdt>'20200315' THEN bal* datediff(DATE'20200315',DATE'20200301')
WHEN begndt <='20200301' AND overdt>'20200301' AND overdt<='20200315' THEN bal* datediff(date(overdt),DATE'20200301')
WHEN begndt >'20200301' AND begndt<='20200315' AND overdt>'20200315' THEN bal* (datediff(DATE'20200315',date(begndt)) +1)
WHEN begndt >'20200301' AND begndt<='20200315' AND overdt<='20200315' THEN bal* datediff(date(overdt),date(begndt))
ELSE 0 END
)
FROM tbo.hs_test2 
WHERE begndt <='20200315' AND overdt>'20200301' 
--AND custno='1001'
GROUP BY custno

