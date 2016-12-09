import java.io._
import org.apache.spark._

  case class Row(url : String, pagerank: Double, urls: Array[String])

  val rawData = sc.textFile("datas/etl-xaaCC-MAIN.txt")

  var lines = rawData.map{ s =>
    val parts = s.split("\t")
    var links = parts(2).substring(1,parts(2).length -1).split(",")
    Row(parts(0), parts(1).toInt, links)
  }

  val contribs = lines.flatMap{ case Row(url,pagerank,urls) =>
    val size = urls.size
    urls.map(url => (url, pagerank / size))
  }

  val newPagerank = contribs.reduceByKey((x,y) => x+y).mapValues(v => 0.15 + 0.85*v)
  newPagerank.sortBy(-_._2).saveAsTextFile("result")