package SparkTest005.Project1Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Exercise1 {
  def main(args: Array[String]) {
    val sc = new SparkContext("local","simpleapp")

    val file = sc.textFile("hdfs://localhost:9000/data/esempio.txt")

    val hobbies = file.flatMap(extractHobby)
      .map(hobby => (hobby, 1))
      .reduceByKey(_ + _)


    val sortedHobbies = hobbies
      .map(_.swap)
      .sortByKey(false)
      .map(_.swap)

    sortedHobbies.saveAsTextFile("hdfs://localhost:9000/data/output/exercise1/")
  }


  def extractHobby(line: String): Array[String] = line.split(" ").drop(0)
}
