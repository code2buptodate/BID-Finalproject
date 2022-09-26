package bdt.cs523;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;


public class SparkStream {
	public static void main(String[] args) throws StreamingQueryException {

		SparkSession spark = SparkSession.builder()
				.appName("Spark Structured Streaming") // Spark Kafka Integration Structured Streaming
				.master("local[*]").getOrCreate();
		
		Dataset<Row> dataSet = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "warInNorthEthiopia")
				.option("failOnDataLoss", "false")
				.load();

		Dataset<Row> twitt = dataSet.selectExpr("CAST(value AS STRING)");
		
		Dataset<Row> schema = twitt
                .selectExpr("value",
                        "split(value,',')[0] as createdAt",
                        "split(value,',')[1] as FollowersCount",
                        "split(value,',')[2] as FavouritesCount",
                        "split(value,',')[3] as Location",
                        "split(value,',')[4] as RetweetCount",
                        "split(value,',')[5] as FavoriteCount",
                        "split(value,',')[6] as Lang").drop("value");
		
		schema = schema
                    .withColumn("createdAt", functions.regexp_replace(functions.col("createdAt")," ", ""))
                    .withColumn("FollowersCount", functions.regexp_replace(functions.col("FollowersCount")," ", ""))
                    .withColumn("FavouritesCount", functions.regexp_replace(functions.col("FavouritesCount")," ", ""))
                    .withColumn("Location", functions.regexp_replace(functions.col("Location")," ", ""))
                    .withColumn("RetweetCount", functions.regexp_replace(functions.col("RetweetCount")," ", ""))
                    .withColumn("FavoriteCount", functions.regexp_replace(functions.col("FavoriteCount")," ", ""))
                    .withColumn("Lang", functions.regexp_replace(functions.col("Lang")," ", ""));

		schema = schema
                    .withColumn("createdAt",functions.col("createdAt").cast(DataTypes.StringType))
                    .withColumn("FollowersCount",functions.col("FollowersCount").cast(DataTypes.IntegerType))
                    .withColumn("FavouritesCount",functions.col("FavouritesCount").cast(DataTypes.IntegerType))
                    .withColumn("Location",functions.col("Location").cast(DataTypes.StringType))
                    .withColumn("RetweetCount",functions.col("RetweetCount").cast(DataTypes.IntegerType))
                    .withColumn("FavoriteCount",functions.col("FavoriteCount").cast(DataTypes.IntegerType))
                    .withColumn("Lang",functions.col("Lang").cast(DataTypes.StringType));

		StreamingQuery query = schema.writeStream().outputMode("append").format("console").start();

		schema.coalesce(1).writeStream().format("csv").outputMode("append").trigger(Trigger.ProcessingTime(10)).option("truncate", false)
        .option("maxRecordsPerFile", 5000).option("path", "hdfs://localhost:8020/user/cloudera/twitterNomoreImport")
	    .option("checkpointLocation", "hdfs://localhost:8020/user/cloudera/twitterNomoreCheckpoint").start().awaitTermination();
		
		query.awaitTermination();

	}

}
