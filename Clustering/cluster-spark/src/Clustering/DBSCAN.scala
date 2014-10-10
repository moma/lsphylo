package Clustering

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.math
import scala.math.Ordering
import utils.terms
import org.apache.spark._
import org.apache.spark.graphx._

object DBSCAN {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("ConnectedComponents (DBSCAN)").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "250")
    val sc = new SparkContext(conf)
  
    val index = utils.terms.index(sc)
  	val topo = utils.parse.getGraph(sc, args)
  					      .map(v => v._1)
  	
  	val nodes = topo.groupByKey().map(v => (v._1, 1)).join(index).map(v => (v._2._2, v._1))
  	val edges = topo.join(index)
  					.map(v => (v._2._1, (v._1, v._2._2)))
  					.join(index)
  					.map(v => Edge(v._2._1._2, v._2._2, None))
  	
  	val graph = Graph(nodes, edges)
  	val cc = graph.connectedComponents().vertices
  	
    val terms = utils.terms.getStrings(sc)
  	
  	val res = nodes.join(cc) // => (Index_N, (TermId_N, CC_N))
  				.map(v => v._2)
  				.join(terms) // => (TermId_N, (CC_N, Label_N))
  				.map(v => v._2)
  				.groupByKey() // => (CC_N, Array[Label])

    res.saveAsTextFile("cooc/DBSCAN/t_" + args(0) + "/" + args(1))
    
  }
}