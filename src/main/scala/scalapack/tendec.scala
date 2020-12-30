package scalapack

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object tendec {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("tendec").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val d = "F:\\Hadoopbigdata\\Spark\\datasets\\wcdata.txt"
    val nrdd = sc.textFile(d)
    val r = nrdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,ascending = false)
    r.collect().foreach(println)
    spark.stop()
  }
}
