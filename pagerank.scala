import java.io._
import org.apache.spark._

  case class Row(url : String, pagerank: Double, urls: Array[String])

  case class Data(data:String)

  val rawData = sc.textFile("datas/etl-xaaCC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.txt")

  println("*************************************debut*************************************")
  var lines = rawData.map{ s =>
    val parts = s.split("\t")
    var links = parts(2).substring(1,parts(2).length -1).split(",")
    Row(parts(0), parts(1).toInt, links)
  }

  val contribs = lines.flatMap{ case Row(url,pagerank,urls) =>
    val size = urls.size
    urls.map(url => (url, pagerank / size))
  }
  val tmp = contribs.reduceByKey((x,y) => x+y).mapValues(v => 0.15 + 0.85*v)

  val output = tmp.collect()

  val pw = new PrintWriter(new File("result.txt"))
  output.foreach(tup => pw.write(tup._1 + " has rank: " + tup._2 + ".\n"))
  pw.close
