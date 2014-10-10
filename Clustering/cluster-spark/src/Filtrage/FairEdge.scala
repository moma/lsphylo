package Filtrage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object FairEdge {
  
  def well1(rkA:Double, wA:Double, rkB:Double, wB:Double): Double = {
	(rkB / wB) / (rkA / wA)
  }
  
  def well2(rkA:Double, wA:Double, rkB:Double, wB:Double): Double = {
	(wB * rkB / rkA) / (wA * rkA / rkB)
  }
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("FairEdge").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "1056")
    val sc = new SparkContext(conf)
    
    val links = utils.parse.cooc(sc, args(0).toInt)
    				 .filter(cooc => cooc._1._1 != cooc._1._2)
    				 //.map(v => v._1)
    				 .cache()
    
    val pertinency = sc.textFile("cooc/pertinency/t_" + args(0)).map(v => (v.split(',')(0), v.split(',')(1).toDouble))
    val density = links.map(v => (v._1._1, v._2))
					   .reduceByKey(_ + _)

	val ranks = pertinency.join(density)

    val fairness = links.map(v => v._1)
    					.join(ranks)
    			   		.map(v => (v._2._1, (v._1, v._2._2)))
    			   		.join(ranks)
    			   		.map(v => ((v._2._1._1, v._1), (v._2._1._2, v._2._2)))
    					.map(v => ((v._1._1, v._1._2), (well1(v._2._1._1, v._2._1._2, v._2._2._1, v._2._2._2), 
    													well2(v._2._1._1, v._2._1._2, v._2._2._1, v._2._2._2))))
    fairness.map(v => v._1._1 + "," + v._1._2 + "," + v._2._1 + "," + v._2._2).saveAsTextFile("cooc/fairness/fairness_" + args(0))
  }

}