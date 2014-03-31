package SparkTest005

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by davide on 31/03/14.
 */
object Exercise2 {
  def main(args: Array[String]) {

    val sc = new SparkContext("local","simpleapp")
    val file = sc.textFile("hdfs://localhost:9000/data/esempio.txt")

    val pairs = file.flatMap(hobby2name).reduceByKey(_ ::: _)

    pairs.saveAsTextFile("hdfs://localhost:9000/data/output/exercise2/")


  }

  def hobby2name(line:String) : List[Tuple2[String,List[String]]] = {
    val name::hobbies = line.split(" ").toList
    hobbies.map(hobby => (hobby, List(name)))
  }

  def names2pairs(names: List[String]): List[Tuple2[String, String]] = {
    var ret = List()
    names.permutations() foreach
  }

}
