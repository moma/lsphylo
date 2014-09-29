package temporalAnalysis

/**
 * Classe Cluster :
 * Un cluster est définit par une année, une liste de terme.
 * 
 * GUICHARD Alexis
 */
class cluster(val line : Array[String])  extends Serializable with Comparable[cluster] {
  
  var id : String = line(1)
  var year : Int =Integer.parseInt(line(0))
  var terms:  List[String] = Nil
  var similarity: Double = 0.0
  for(  i <- List.range(2, line.length)) terms = line(i)::terms

  override def toString(): String = {
  	var s: String= ""
  	  print("Cluster "+id+" a l'année "+ year+" avec sim "+similarity)
    terms.foreach(t => print(t+" "))
  	return s
  }
  
  def addTerms(listTerm:List[String]){
    for(i<- List.range(0, listTerm.length))
      terms = listTerm(i)::terms
  }
 
  def compare(that:cluster) : Int = {
     if (this.similarity > that.similarity)
	         return  1;
		 else if (this.similarity < that.similarity)
	         return -1;
		return 0;
  }
  def compareTo(that: cluster): Int = compare(that)
  def <  (that: cluster): Boolean = (this.compare(that)) <  0
  def >  (that: cluster): Boolean = (this compare that) >  0
  def <= (that: cluster): Boolean = (this compare that) <= 0
  def >= (that: cluster): Boolean = (this compare that) >= 0
	
  
}