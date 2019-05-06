import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object PageRank extends App {
  def main(args:Array[String]):Unit={
  val d = 0.85
  val itr_num = 10
  val sparkConf = new SparkConf().setAppName("Page Rank").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val text = sc.textFile("/home/narip/hadoop_exp/DataSet.txt")

  val links = text.map(line => (line.split("\t")(0),line.split("\t")(1).split(","))).persist()
  var ranks = links.mapValues(x => 1.0)

  for(i <- 1 to itr_num){
    val contribs = links.join(ranks).flatMap{
      case(url,(links,rank)) => links.map(dest => (dest,rank/links.size))
    }

    ranks = contribs.reduceByKey((x,y) => x+y).mapValues(x => (1-d) + d * x)
  }
  ranks = ranks.sortBy(_._2,false)
  var res = ranks.map(x => (x._1,x._2.formatted("%.10f")))
  res.saveAsTextFile("/home/narip/ranks")
  }
}