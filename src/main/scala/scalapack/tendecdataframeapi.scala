package scalapack

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object tendecdataframeapi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("tendecdataframeapi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    import spark.sql

    val d="F:\\Hadoopbigdata\\Spark\\datasets\\10000Records.csv"

    val df = spark.read
      .format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("delimiter",",")
      .option("inferSchema", "true") // Automatically infer data types
      .load(d)
      val a = df.columns.map(x=>x.toLowerCase.replaceAll("[^a-zA-Z]",""))
    val df1 =df.toDF(a:_*)
   df1.show()
   df1.printSchema()

    spark.stop()
  }
}
