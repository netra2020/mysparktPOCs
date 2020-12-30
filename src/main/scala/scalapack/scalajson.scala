package scalapack

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object scalajson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("scalajson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data ="F:\\Hadoopbigdata\\Spark\\datasets\\companies.json"
    val df = spark.read.format("json").load(data)
    df.createOrReplaceTempView("t")
    df.printSchema()
   // val r = spark.sql("select `_id`.`$oid` oid ,acquisition.* , acquisition.acquiring_company.* from t")
   // r.show(4)

    val a =spark.sql("select s.* from t lateral view explode (relationships)rp as s")
    a.show(4)
    spark.stop()
  }
}
