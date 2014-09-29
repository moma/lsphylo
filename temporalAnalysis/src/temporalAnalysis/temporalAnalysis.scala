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
 *	 - file-input : fichier contenant les clusters
 *   - seuil : seuil de filtrage
 * 	 - intervalle : Considérer le calcul de similarité sur plusieurs années exemple : intervalle = 1, si aucune similarité trouvée pour 1990 alors calcul sur l'année 1991 ( intervalle = 2 recherche sur 1992 si 1991 infructueuse)
 *	 - year-start / year-end : Année de départ et l'année de fin pour la recherche de similarité ( ex : 1990 et 2010 pour une détection de transitions sur la période 1990 à 2010)
 * Sortie : résultat des meilleures similarités pour un cluster prise en compte de l'année t+2 si aucune similarité au temps t+1. Chaque ligne peut être : 
 * 			- ID-CLUSTER ANNEE-T	ID-CLUSTER ANNEE-T+1 
 * 
 * 	GUICHARD Alexis
 * */
	def main(args: Array[String]) 
	{
		if(args.length != 6){
			System.err.println("Usage: TemporalAnalysis <file-input> <file-output> <seuil> <intervalle> <year-start> <year-end> ");
			System.exit(6);
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
		for(year <- Integer.parseInt(args(4)) to Integer.parseInt(args(5))){
			val clustersT = clusters.filter(c => c.year.equals(year)) // clusters temps T
			for(c <- clustersT) {
			  var flag:Boolean = false;
			  var intervalle = 1;
			  breakable{
			    do{
			  
				flag  = analysis(c,clusters.filter(clusters2 => clusters2.year.equals(year+intervalle)), args(2).toDouble, bw);
				if(flag)
				  break;
				intervalle= intervalle+1;
				println(Integer.parseInt(args(3)))
			    
			  }while(intervalle<=Integer.parseInt(args(3)) )
			}
			  
			} 

		}
		bw.close

	}

	/**
	 * Détection de la meilleure similarité pour un cluster c
	 */
	def analysis( c:cluster, clusters:List[cluster], seuil:Double, file:BufferedWriter) : Boolean = {

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

		var s:Double = bestSim.similarity
		for(c2 <- clusterSim){
			breakable{

				// Calcul de toutes les unions possibles avec c2
			var tempClusterSim: List[cluster]  = calculUnionSimilarity(c, c2, clusterSim, bestSim.similarity);
			var tempSim: Double = clusterUnionSimilarity(c, tempClusterSim);

			if(tempClusterSim.length> 0 && tempSim >= s ) {
				resultSim = tempClusterSim
				s = tempSim

			}
			}
		}

	}
	// similarité par l'union
	if(resultSim.length > 0 && clusterUnionSimilarity(c, resultSim) >= seuil ) {

		file.write(c.id+" "+c.year+" "+resultSim.apply(0).id+" "+resultSim.apply(0).year);
		file.newLine()
		file.write(c.id+" "+c.year+" "+resultSim.apply(1).id+" "+resultSim.apply(1).year);
		file.newLine()

		return true
	}
	// Similarité individuelle trouvé
	if(bestSim != null && clusterSimilarity(c, bestSim) >= seuil ) {

		file.write(c.id+" "+c.year+" "+bestSim.id+" "+bestSim.year);
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