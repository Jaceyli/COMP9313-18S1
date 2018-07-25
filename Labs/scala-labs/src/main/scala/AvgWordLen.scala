import org.apache.spark.{SparkConf, SparkContext}

/** Give a collection of documents.
  * Compute the average length of words starting with each letter.
  * Sorted by avgLen descending order
  *
  * created by Jingxuan Li on 24/6/18
  */
object AvgWordLen {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile)
    val words = textFile.flatMap(_.split(" "))
    val counts = words.filter(x=>x.length>1 && x.charAt(0) <='z' && x.charAt(0) >= 'a')
                      .map(x=>(x.charAt(0), (x.length,1)))
    val avgLen = counts.reduceByKey((a,b)=>(a._1 + b._1, a._2 + b._2))
//      .mapValues(x=> x._1.toDouble / x._2)
//      or
      .map(x=>(x._1, x._2._1.toDouble / x._2._2))
      .sortBy(_._2,false)
      .map(x=>x._1 + "\t" + x._2)

    avgLen.saveAsTextFile(outputFile)

  }
}
