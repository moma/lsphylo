package utils

import org.apache.spark.SparkContext._

object parse {
	
	def stripChars(s:String, ch:String)= s filterNot (ch contains _)
  
	def cooc(sc : org.apache.spark.SparkContext, y : Int) = 
	  sc.textFile("cooc/output/cooc_" + y.toString).map( {
		l => var c = l
			if(l.contains('(')) {
				c = stripChars(l, "() u'")
			}
		((c.split(",")(0), c.split(",")(1)), c.split(",")(2).toDouble)
	}).cache()
	
	def fairness(sc : org.apache.spark.SparkContext, y : Int) = 
	  sc.textFile("cooc/fairness/fairness_" + y.toString).map( {
		l => var c = l
			if(l.contains('(')) {
				c = stripChars(l, "() u'")
			}
		((c.split(",")(0), (c.split(",")(1))), c.split(",")(3).toDouble)
	}).cache()
	
	def filtered(sc : org.apache.spark.SparkContext, y : Int, p : Double) =
	  fairness(sc, y).filter(v => math.abs(math.log10(v._2)) < p)
	  
	def getGraph(sc : org.apache.spark.SparkContext, args : Array[String]) = {
	  if (args.length > 1 && args(2) != "inf") {
	    filtered(sc, args(0).toInt, args(1).toDouble)
	  } else {
	    cooc(sc, args(0).toInt)
	  }
	}
}