package scalapack

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object tendecflatmap {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("tendecflatmap").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    /*
    val sales=sc.parallelize(List(
      ("tg",  "tv",  10.0, 1),
      ("tg",  "tv",  20.0, 2),
      ("tg",  "fri", 30.0, 3),
      ("tg", "fri", 40.0, 4),
      ("tg", "mob", 50.0, 5),
      ("tg",  "mob", 60.0, 6)))
    val a= sales.toDF("state","item","amt","unit")
    a.createOrReplaceTempView("tab")
    val res = spark.sql("select state,item,sum(amt) ,sum (unit )from tab group by state,item")
    res.show()
    */


    spark.stop()
  }
}
