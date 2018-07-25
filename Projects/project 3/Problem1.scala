package comp9313.ass3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by Jingxuan Li on 28/4/18
  */


object Problem1 {

    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("Problem1").setMaster("local")
      val sc = new SparkContext(conf)
      val input = sc.textFile(inputFile)
      val vertex = input.map(x => (x.split(" ")(1).toInt,(x.split(" ")(3).toDouble, 1)))
        .reduceByKey((x,y)=> (x._1 + y._1, x._2 + y._2))
//      compute the average length of the out-going edges for each node
        .map(t => (t._1, t._2._1/t._2._2))
//      Remove the nodes that have no out-going edges
        .filter(_._2 != 0)
//      when values are same, sort according to the node IDs (numeric value) in ascending order
        .sortByKey(ascending = true)
//      sort the nodes according to the average lengths in descending order
        .sortBy(_._2 , ascending = false)
        .map(x => x._1 + "\t" + x._2)
      vertex.saveAsTextFile(outputFile)
    }
}
