package temporalAnalysis

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import java.io.BufferedReader
import java.io.FileReader
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.ResultSet
import java.sql.Statement
import scala.util.control.Breaks._
object temporalAnalysis {
/**
 * Analyse temporelle entre clusters
 * Entrée : ensemble des clusters de différentes années. une ligne représente un cluster : ANNEE ID-CLUSTER [liste des termes]
 * Sortie : résultat des meilleures similarités pour un cluster prise en compte de l'année t+2 si aucune similarité au temps t+1. Chaque ligne peut être : 
 * 			- ID-CLUSTER+ANNEE-T	ID-CLUSTER+ANNEE-T+1 => similarité individuelle
 *    		- ID-CLUSTER+ANNEE-T	ID-CLUSTER+ANNEE-T+1	ID-CLUSTER+ANNEE-T+1 => similarité par l'union de deux clusters
 */
	def main(args: Array[String]) 
	{
		if(args.length != 2){
			System.err.println("Usage: TemporalAnalysis <file-input> <file-output>");
			System.exit(2);
		}
		/*
		// Connexion à la base de donéne
		Class.forName("postgresql.Driver");
		val url:String = "jdbc:postgresql:"+args(0)
		val con:Connection = DriverManager.getConnection(url, args(1), args(2));
		var résultats:ResultSet = null;
		val requete:String = "SELECT * FROM client";

		try {
			val stmt:Statement = con.createStatement();
			résultats = stmt.executeQuery(requete);
		}

		 */

		val list = read(new BufferedReader(new FileReader(args(0))), Nil).reverse;

		val clusters : List[cluster] = list.map(f => new cluster(f.split(" ")));
		val bw = new BufferedWriter(new FileWriter(new File(args(1))));
		for(year <- 1990 to 2010){
			val clustersT = clusters.filter(c => c.year.equals(year)) // clusters temps T
			for(c <- clustersT) {
				val flag:Boolean = analysis(c,clusters.filter(clusters2 => clusters2.year.equals(year+1)), bw);
				if(!flag)
					analysis(c,clusters.filter(c2 => c2.year.equals(year+2)), bw);
			} 

		}
		bw.close

	}

	/**
	 * Détection de la meilleure similarité pour un cluster c
	 */
	def analysis( c:cluster, clusters:List[cluster], file:BufferedWriter) : Boolean = {

			var clusterSim:List[cluster] = Nil;
	var bestSim:cluster = null;
	// calcul similarité individuelle
	clusters.foreach(c2 => {
		val sim:Double = clusterSimilarity(c, c2)
				if(sim >= 0.1){
					clusterSim = c2::clusterSim;
					c2.similarity = sim
				}
	if(bestSim == null || sim > bestSim.similarity){
		bestSim = c2
				bestSim.similarity = c2.similarity
	}
	})
	// calcul union
	var resultSim: List[cluster] = Nil
	if(clusterSim != null && bestSim != null && bestSim.similarity < 1.0){
		clusterSim.sorted

		var seuil:Double = bestSim.similarity
		for(c2 <- clusterSim){
			breakable{

				// Calcul de toutes les unions possibles avec c2
			var tempClusterSim: List[cluster]  = calculUnionSimilarity(c, c2, clusterSim, bestSim.similarity);
			var tempSim: Double = clusterUnionSimilarity(c, tempClusterSim);

			if(tempClusterSim.length> 0 && tempSim >= seuil ) {
				resultSim = tempClusterSim
				seuil = tempSim

			}
			}
		}

	}
	// similarité par l'union
	if(resultSim.length > 0) {

		file.write(c.id+" "+resultSim.apply(0).id+" "+resultSim.apply(1).id);
		file.newLine()

		return true
	}
	// Similarité individuelle trouvé
	if(bestSim != null) {

		file.write(c.id+" "+bestSim.id);
		file.newLine()

		return true
	}

	return false
	}

	/**
	 * Méthode pour lire un fichier
	 */
	def read(buf : BufferedReader, acc : List[String] ) : List[String] = buf.readLine match {
	case null => acc
	case s => read(buf, s::acc)  // Appel recursif optimisé par Scala
	}

	/**
	 * Méthode pour calculer la similarité de tous les unions de c2 avec un autre cluster par rapport au cluster cible c
	 */
	def calculUnionSimilarity(c:cluster, c2: cluster, clusterSim: List[cluster], seuil:Double): List[cluster] = {
			var resultSim:List[cluster] = Nil;
	val index: Int = clusterSim.indexOf(c2);
	for(i <- index+1 to clusterSim.length-1){
		val candidats:List[cluster] = List(c2,clusterSim.apply(i));
	val sim: Double = clusterUnionSimilarity(c, candidats);
	if(sim > seuil)
		resultSim = candidats;
	else if(i+1 < clusterSim.length && c2.similarity+clusterSim.apply(i+1).similarity< seuil)
		break;

	}
	return resultSim
	}

	/**
	 * Similarité entre deux clusters 
	 * Similarité Jaccard
	 */
	def clusterSimilarity(c1:cluster , c2:cluster): Double ={

			var terms1 = c1.terms;
			var terms2 = c2.terms;
			var sizec1 = terms1.size;

			var intersect = terms1.intersect(terms2);
			var union = terms2.size+terms1.size - intersect.size;

			return  intersect.size.toDouble/union.toDouble;

	}

	/**
	 * Similarité entre un cluster c1 et l'union de plusieurs clusters
	 * (Similarité Jaccard)
	 */
	def clusterUnionSimilarity(c1:cluster , clusters:List[cluster]): Double ={

			var terms1 = c1.terms
					var terms2 = List[String]();
			for(c <- clusters)
				terms2 = terms2.:::(c.terms)

				var sizec1 = terms1.size

				var intersect = terms1.intersect(terms2)
				var union = terms2.size+terms1.size - intersect.size

				return  intersect.size.toDouble/union.toDouble;

	}


}