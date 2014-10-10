package utils
import org.apache.spark.SparkContext._

object terms {
	def getStrings(sc : org.apache.spark.SparkContext) = sc.textFile("cooc/terms").map(v => ( utils.parse.stripChars(v.split('|')(0), "()'u "), v.split('|')(1) )).cache()
	def index(sc : org.apache.spark.SparkContext) = sc.textFile("cooc/index").map(v => ( utils.parse.stripChars(v.split(',')(0), "()'u "), v.split(',')(1).toLong )).cache() 
}