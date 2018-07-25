package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import collection.mutable.ArrayBuffer
import collection.mutable.HashSet

object SetSimJoin {
 
  def f(idx: Int, arr: Array[Int], t: Double): ArrayBuffer[(Int, (Int, Array[Int]))] = {
	val prefix = ((1-t) * arr.length).toInt
	val ab = new ArrayBuffer[(Int, (Int, Array[Int]))]()
	for(i <- 0 to prefix){
  	ab.append( (arr(i), (idx, arr)) )
	}
	return ab;
  }
 
  def compute(a1: Array[Int], a2: Array[Int], key: Int): Double = {
	var sim = 0.0
	val dict = new HashSet[Int]
	for(i <- 0 to a1.size-1){
  	dict+=a1(i)
	}
	var intersect = 0
	var union = a1.size
	var flag = false    
	for(i <- 0 to a2.size-1){
  	if(dict.contains(a2(i))){
    	if(!flag){
      	if(key == a2(i)) flag = true
      	else  return -1
    	}
    	intersect = intersect + 1
  	} else {
    	union += 1
  	}
	}
	sim = intersect.toDouble/union
	return sim
  }
 

  def sim(rec: Array[(Int, Array[Int])], t: Double, key: Int): ArrayBuffer[(Int, Int, Double)] = {
	val pairs = new ArrayBuffer[(Int, Int, Double)]()
	for(i <- 0 to rec.size-1){
  	for(j <- i+1 to rec.size-1){
    	val rec1 = rec(i)._2
    	val rec2 = rec(j)._2
    	val similarity = compute(rec1, rec2, key)
    	if(similarity >= t){
      	pairs.append((rec(i)._1, rec(j)._1, similarity))
    	}
  	}
	}
	return pairs;
  }
 
  def main(args: Array[String]) {
	val inputFile = args(0)
	val outputFolder = args(1)
	val threshold = args(2).toDouble    
    
	val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
	val sc = new SparkContext(conf)
	val input = sc.textFile(inputFile)
       	 
	val records = input.map(line=>line.split(" ")).map(arr => arr.map(_.toInt)).map( arr => (arr(0), arr.slice(1, arr.length)))
    
	var i=0
	val pairs = records.flatMap{case(idx, arr) => f(idx, arr, threshold)}
	val joinres = pairs.groupByKey().flatMap{case(key, rec) => sim(rec.toArray, threshold, key)}
	val finalres = joinres.sortBy(_._2.toInt).sortBy(_._1.toInt).map(x => "(" + x._1 + ","+x._2+")\t"+x._3)

	finalres.saveAsTextFile(outputFolder)
  }
}



