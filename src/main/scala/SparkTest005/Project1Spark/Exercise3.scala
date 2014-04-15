package SparkTest005.Project1Spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by davide on 03/04/14.
 */
object Exercise3 {
  def main(args: Array[String]) {



    val sc = new SparkContext("spark://ec2-54-185-216-73.us-west-2.compute.amazonaws.com:7077", "Ex3",
      System.getenv("SPARK_HOME"), Seq("s3n://littledatabucket/jars/SparkTest005.jar"))


    val file = sc.textFile("s3n://littledatabucket/input/esempio5k.txt")

    val pairs = file.flatMap(hobby2name).reduceByKey(_ ::: _)
    val result = pairs.flatMap(couple2hobby).reduceByKey(_ ::: _).map(x => "%s %s".format(x._1, x._2.mkString(" ")))

    result.saveAsTextFile("s3n://littledatabucket/output/es3/")
  }

  def hobby2name(line:String) : List[Tuple2[String,List[String]]] = {
    val name::hobbies = line.split(" ").toList
    hobbies.map(hobby => (hobby, List(name)))
  }

  def couple2hobby(couple: Tuple2[String, List[String]]): List[Tuple2[String, List[String]]] = {
    val (hobby, names) = couple
    names.combinations(2).toList.map(pair => ("%s,%s".format(pair(0), pair(1)), List(hobby)))
  }
}