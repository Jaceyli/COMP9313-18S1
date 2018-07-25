package comp9313.ass4

import org.apache.spark.{SparkConf, SparkContext}

/**
  *  Set Similarity Join
  *
  *  Stage 1: Sort tokens by frequency
  *  Stage 2: Find “similar” id pairs
  *  Stage 3: Remove Duplicates
  *
  * created by Jingxuan Li on 28/5/18
  */
object SetSimJoin {

  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFile = args(1)
    val threshold = args(2).toDouble
    //
    val conf = new SparkConf().setAppName("SetSimJoin")

    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)

//  compute tokens frequency
    val freq = input.flatMap(_.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
      .sortBy(_._2)
      .keys
      .toLocalIterator
      .toList

//    print(freq)

//    using prefixes
    input.zipWithIndex.flatMap({
      case (line, index) =>
        val token = line.split(" ").toIndexedSeq.drop(1).toList
//        unsorted
//        token.dropRight(math.ceil(token.length * threshold).toInt - 1)

//      sort tokens by frequency
        val sortedtoken = freq.flatMap( id => token.find(x => x == id ))
//      compute prefix length
        sortedtoken.dropRight(math.ceil(token.length * threshold).toInt - 1)
          .map {
            (_, List((index, token)))
          }
    })
      .reduceByKey(_ ::: _)
//      filter at least 2 R
      .filter(_._2.length > 1)
      .flatMapValues(_.combinations(2))
      .values

//     size filter
      .filter({
            case List((a, b), (c, d)) =>
              b.length >= d.length * threshold && d.length >= b.length * threshold
         })
//    compute Jaccard similarity
      .mapPartitions(line => {
        line.map({
            case List((a, b), (c, d))=>
              val sim = b.intersect(d).distinct.length.toDouble / b.union(d).distinct.length.toDouble
              (a, c) -> sim
          })
      })

//     remove duplicates
      .reduceByKey((v1, v2) => v1)

//     filter similary > threshold
      .filter(_._2 >= threshold)
      .sortBy(_._2)
      .sortByKey()
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(outputFile)
  }
}
