import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Given a large text file, your task is to find out the top-k most frequent co-occurring term pairs.
  * The co-occurrence of (w, u) is defined as: u and w appear in the same line (this also means that (w, u)
  * and (u, w) are treated equally).
  *
  * created by Jingxuan Li on 24/6/18
  */
object CoTermFreq {
  def pairGen(wordArray: Array[String]) : ArrayBuffer[(String, Int)] = {
    val abuf = new ArrayBuffer[(String, Int)]
    for(i<- 0 to wordArray.length - 1){
      val term1 = wordArray(i)
      if(term1.length() > 0){
        for(j <- i + 1 to wordArray.length - 1){
          val term2 = wordArray(j)
          if(term2.length() > 0){
            //consider the order of term1 and term2
            if(term1 < term2){
              abuf.+=:(term1 + "," + term2, 1)
            }else{
              abuf.+=:(term2 + "," + term1, 1)
            }
          }
        }
      }
    }
    abuf
  }


  def main(args: Array[String]) {
    val inputFile = args(0)
    //    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("CoTermFreq").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile)
    val words = textFile.map(x=>x.split(" "))

    val pair = words.flatMap(x=>pairGen(x)).reduceByKey(_+_)
    val topk = pair.map(_.swap).sortByKey(false).take(20).map(_.swap)

    topk.foreach(x => println(x._1, x._2))
  }
}
