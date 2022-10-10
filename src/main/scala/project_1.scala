import org.apache.spark.{SparkConf, SparkContext}

object project_1
{
  def main(args: Array[String]): Unit =
  {
     var conf = new SparkConf().setMaster("local[*]").setAppName("firstScalaProject")
     var sc = new SparkContext(conf)


    var data = sc.textFile("D:\\gitfolder/datafile")

    var mapdata=data.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)

    var result = mapdata.sortBy(x => x._1)

    result.collect.foreach(println)

  }
}
