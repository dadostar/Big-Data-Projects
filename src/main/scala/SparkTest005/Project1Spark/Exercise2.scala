package SparkTest005.Project1Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Exercise2 {
  def main(args: Array[String]) {

    val sc = new SparkContext("local","simpleapp")
    val file = sc.textFile("hdfs://localhost:9000/data/esempio.txt")

    val pairs = file.flatMap(hobby2name).reduceByKey(_ ::: _)
    val result = pairs.flatMap(pair => names2pairs(pair._2)).reduceByKey(_ + _).map(_._1)

    result.saveAsTextFile("hdfs://localhost:9000/data/output/exercise2/")


  }

  def hobby2name(line:String) : List[Tuple2[String,List[String]]] = {
    val name::hobbies = line.split(" ").toList
    hobbies.map(hobby => (hobby, List(name)))
  }

  def names2pairs(names: List[String]): List[Tuple2[String, Int]] = {
    names.combinations(2).toList.map(couple => ("%s, %s".format(couple(0), couple(1)), 0))
  }
}
