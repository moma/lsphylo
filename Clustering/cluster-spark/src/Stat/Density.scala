package Stat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object Density {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Density").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "1056")
    val sc = new SparkContext(conf)

    val g = utils.parse.getGraph(sc, args).filter(v => v._1._1 != v._1._2)
    
	println( "Edge density: \n" +
		 g.map(t => (t._1._1, 1.0))
		  .reduceByKey(_ + _)
		  .map(c => c._2)
		  .stats()
		  .toString
	)
		  
	sc.stop()
  }
	
}