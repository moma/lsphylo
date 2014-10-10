package Clustering

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.math
import scala.math.Ordering
import org.apache.spark._
import org.apache.spark.graphx._

object CliqueFind {
  /*
  def stripChars(s:String, ch:String)= s filterNot (ch contains _)

  def vprog[A](vid: VertexId, vd: VD, msg: A) = {
    
  }
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("ConnectedComponents (DBSCAN)").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "250")
    val sc = new SparkContext(conf)
  
    val TermsUtility = new Terms
    val index = TermsUtility.index(sc)
  	val fairness = sc.textFile("cooc/fairness/fairness_" + args(0))
  					 .map(l => (stripChars(l.split(',')(0), "u'"), (stripChars(l.split(',')(1), "u'"), l.split(',')(3).toDouble)))
  					 .filter(v => math.abs(math.log10(v._2._2)) < args(1).toDouble)
  					 .mapValues(v => v._1)
  	
  	val nodes = fairness.groupByKey().map(v => (v._1, 1)).join(index).map(v => (v._2._2, v._1, true, Iterable(new Color(v._2._2)), Iterable()))
  	val edges = fairness.join(index)
  					 	.map(v => (v._2._1, (v._1, v._2._2)))
  					 	.join(index)
  					 	.map(v => Edge(v._2._1._2, v._2._2, None))
  }
  Pregel
  */
}