/**
  * created by Jingxuan Li on 24/6/18
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)

    val counts = input.flatMap(line=>line.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    counts.saveAsTextFile(outputFolder)
  }
}