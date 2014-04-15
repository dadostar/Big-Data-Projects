package SparkTest005.Project1Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Exercise1 {
  def main(args: Array[String]) {
    val sc = new SparkContext("spark://ec2-54-185-216-73.us-west-2.compute.amazonaws.com:7077", "Ex3",
      System.getenv("SPARK_HOME"), Seq("s3n://littledatabucket/jars/SparkTest005.jar"), null, null)

    val file = sc.textFile("s3n://littledatabucket/input/esempio5k.txt")

    val hobbies = file.flatMap(extractHobby)
      .map(hobby => (hobby, 1))
      .reduceByKey(_ + _)


    val sortedHobbies = hobbies
      .map(_.swap)
      .sortByKey(false)
      .map(_.swap)

    sortedHobbies.saveAsTextFile("s3n://littledatabucket/output/es2/")
  }


  def extractHobby(line: String): Array[String] = line.split(" ").drop(0)
}
