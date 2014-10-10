package Stat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object Pertinency {
  
  def stripChars(s:String, ch:String)= s filterNot (ch contains _)
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Pertinency (PageRank)").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "1056")
    val sc = new SparkContext(conf)

	/*
	 * That's a weighted PageRank based on org.apache.spark.examples.SparkPageRank
	 * Dunno if I should put this under Apache License, Version 2.0 then...
	 */
    
    val links = utils.parse.cooc(sc, args(0).toInt)
    	  .filter(cooc => cooc._1._1 != cooc._1._2)
    	  .map(v => (v._1._1, (v._1._2, v._2)))
		  .groupByKey()
		  .cache()
	val clinks = links.count()
	var ranks = links.mapValues(v => (1.0 / clinks, v.map(w => w._2).sum))
	
	val iters = args(1).toInt
	for (i <- 1 to iters) {
	  val contribs = links.join(ranks).values.flatMap{
	    case (linkOut, rankW) =>
	      /*
	       * 	Here we have:
	       *  		for any node N (implicit, it's links.join(ranks).key):
	       *    		linkOut: list of weighted (unnormalized) edges out of N in the form [(N_1, W_1), ...]
	       *      		rankW: a couple, ._1 is the current rank of N, and ._2 the normalization weight of edges out of N
	       */
	      linkOut.map( l => (l._1, rankW._1 * l._2 / rankW._2) ) // Contribution of N on l._1
	  }
	  ranks = contribs.reduceByKey(_ + _).mapValues(r => 0.15 / clinks + 0.85 * r).join(ranks.mapValues(r => r._2)) // The join here is just to maintain the normalization weight attached to each node 
	}
    ranks.map(res => res._1 + "," + res._2._1.toString)
    	 .saveAsTextFile("cooc/pertinency/t_" + args(0))
    	 
    sc.stop()
  }
}