package scalapack

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mapfilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data ="F:\\Hadoopbigdata\\Spark\\datasets\\cust_spent.csv"
    val ardd =sc.textFile(data)
    //val pro = ardd.filter(x=>x!=x.contains("age"))
    val header = ardd.first()
   val pro2 = ardd.filter(x=>x!=header).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=> x._3=="blr" && x._2<=30 && x._1=="aravind")
   // pro.take(num = 9).foreach(println)
    //val pro2 = ardd.filter(x=>x!=header).map(x=>x.split(",")).map(x=>(x(1),x(2).toInt,x(3))).filter(x=> max('SpentMoney'=1000)).groupBy(x=> x._3)
   pro2.collect.foreach(println)
    spark.stop()
  }
}
