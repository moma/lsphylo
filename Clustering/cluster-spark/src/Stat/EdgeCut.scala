package Stat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.math

object EdgeCut {
	
  def stripChars(s:String, ch:String)= s filterNot (ch contains _)
  
  def main(args: Array[String]): Unit = {  
	val conf = new SparkConf().setAppName("EdgeCut").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "250")
    val sc = new SparkContext(conf)
	val prev = utils.parse.fairness(sc, args(0).toInt).count()
  	val next = utils.parse.filtered(sc, args(0).toInt, args(1).toDouble).count()
  	
  	println("Edge cut: "+ (prev - next).toString + " / " + prev.toString + " (" + (math.floor(10000 * (prev - next) / prev ) /100).toString + "%)")
  }
}