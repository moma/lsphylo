package Stat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object Popularity {
  
  def stripChars(s:String, ch:String)= s filterNot (ch contains _)
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Popularity").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "1056")
    val sc = new SparkContext(conf)

	val lines = utils.parse.cooc(sc, args(0).toInt)
	val popu = lines.filter(cooc => cooc._1._1 == cooc._1._2)
					.map(cooc => cooc._1._1 + "," + cooc._2.toString)
	popu.saveAsTextFile("cooc/popularity/t_" + args(0))
	
	sc.stop()
	}
}