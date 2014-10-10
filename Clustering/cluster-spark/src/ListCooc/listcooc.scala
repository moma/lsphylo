package ListCooc

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object Main {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("ListCooc").setMaster("spark://132.227.199.101:7077").set("spark.default.parallelism", "5000")
		
		// Tuning
		conf.set("spark.shuffle.consolidateFiles", "true")
		//conf.set("spark.storage.memoryFraction", "0.3")
		//conf.set("spark.shuffle.memoryFraction", "0.5")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		conf.set("spark.kryo.referenceTracking", "false")
		
		val sc = new SparkContext(conf)

		val lines = sc.textFile("cooc/input/oc_" + args(0) +".csv").map(l => (l.split(",")(0), utils.parse.stripChars(l.split(",")(1), "()'u "))) 
		val res = lines.join(lines)
		               .map( a => (a._2, 1) )
		  			   .reduceByKey(_ + _)
					   .map(cooc => cooc._1._1 + "," + cooc._1._2 + "," + cooc._2.toString)
		res.saveAsTextFile("cooc/output/cooc_" + args(0))
	}
}
