package com.availity.spark.provider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProviderRoster  {

  def main(args: Array[String]): Unit = {}

  val spark = SparkSession.builder()
    .appName("provider")
    .master("local[*]")
    .getOrCreate()

  // Define schema for Visits.csv
  val visitsSchema = StructType(Array(
    StructField("visit_iD", StringType, nullable = false),
    StructField("provider_id", StringType, nullable = false),
    StructField("visit_date", DateType, nullable = false)
  ))

  // Read Visits.csv
  val visitsDF = spark.read
    .option("delimiter", ",")
    .schema(visitsSchema)
    .csv("data/visits.csv")
    .withColumn("visit_month", month(col("visit_date")))
    .withColumn("visit_year", year(col("visit_date")))
    .filter("provider_id = '25024'")

// Read provider.csv
  val providerDF = spark.read.option("header","true").option("delimiter","|").csv("data/providers.csv")


  // Task 1: Calculate total number of visits per provider per specialty
  val visitsPerProviderPerSpecialty = providerDF
    .join(visitsDF, Seq("provider_id"))
    .groupBy("provider_id", "provider_specialty", "first_name", "middle_name", "last_name")
    .agg(count("visit_date").alias("total_visits"))

  // Write the result partitioned by provider's specialty
  visitsPerProviderPerSpecialty
    .write.mode("overwrite")
    .partitionBy("provider_specialty")
    .json("output/visits_per_provider_per_specialty")

  // Task 2: Calculate total number of visits per provider per month
  val visitsPerProviderPerMonth = visitsDF
    .groupBy("provider_id",   "visit_month", "visit_year")
    .agg(count("visit_date").alias("total_visits"))

  // Write the result in JSON format
  visitsPerProviderPerMonth
    .write.mode("overwrite")
    .json("output/visits_per_provider_per_month")

}
