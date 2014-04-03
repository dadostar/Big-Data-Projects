package SparkTest005.Project1Spark

import org.apache.spark.SparkContext

/**
 * Created by davide on 03/04/14.
 */
object Exercise3 {
  def main(args: Array[String]) {

    val sc = new SparkContext("local","simpleapp")
    val file = sc.textFile("hdfs://localhost:9000/data/esempio.txt")




    //result.saveAsTextFile("hdfs://localhost:9000/data/output/exercise2/")


  }
}