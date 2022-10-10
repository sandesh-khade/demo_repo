import org.apache.spark.{SparkConf, SparkContext}

object Scala_Project
{
  def main(args: Array[String]): Unit =
  {
    var conf = new SparkConf().setMaster("local[*]").setAppName("SparkFirstJob")
    var sc =new SparkContext(conf)


    var file =sc.textFile("datafile.txt")

    var mapdata=file.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)

    var result = mapdata.sortBy(x => x._1)

    result.collect.foreach(println)
  }
}
