package comp9313.ass3

import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * created by Jingxuan Li on 29/4/18
  */
object Problem2 {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val sourceid = args(1).toInt
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)


    // A graph with edge attributes containing distances
    val vRDD: RDD[(VertexId, Int)] = input.map(line => line.split(" ")(1).toInt)
      .zipWithIndex()
      .map(_.swap)

    val eRDD: RDD[Edge[Double]] = input.map(line => {
      line.split(" ") match {
        case Array(n1, n2, n3, n4) => Edge(n2.toInt, n3.toInt, n4.toDouble)
      }
    })
    val graph = Graph(vRDD, eRDD)

//    graph.edges.foreach(println)
//    graph.vertices.foreach(println)

    val sourceId: VertexId = sourceid // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph: Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)
//      println(sssp.vertices.collect.mkString("\n"))

      //count the path that is not Infinite, then - 1 (itself)
      val count = sssp.vertices.count() - sssp.vertices.filter(_._2._1 isInfinity).count() - 1
      println(count)
  }
}
