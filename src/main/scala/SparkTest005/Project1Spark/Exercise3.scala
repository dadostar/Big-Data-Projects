package SparkTest005.Project1Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by davide on 03/04/14.
 */
object Exercise3 {
  def main(args: Array[String]) {
    val sc = new SparkContext("local","simpleapp")
    val file = sc.textFile("hdfs://localhost:9000/data/esempio.txt")

    val pairs = file.flatMap(hobby2name).reduceByKey(_ ::: _)
    val result = pairs.flatMap(couple2hobby).reduceByKey(_ ::: _).map(x => "%s %s".format(x._1, x._2.mkString(" ")))

    result.saveAsTextFile("hdfs://localhost:9000/data/output/exercise3/")
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