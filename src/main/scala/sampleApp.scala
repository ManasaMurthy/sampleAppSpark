import org.apache.spark.{SparkConf, SparkContext}

object sampleApp {

  def main(args:Array[String]): Unit ={

  val conf = new SparkConf().setMaster("local[2]").setAppName("Sample")

    val sc = new SparkContext(conf)

    val data = List("marriott hotel", "hotel marriott")

    sc.parallelize(data).flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x+y).foreach(println)

  }

}
