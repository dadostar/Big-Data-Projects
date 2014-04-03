package SparkTest005.Project1Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Exercise2 {
  def main(args: Array[String]) {

    val sc = new SparkContext("local","simpleapp")
    val file = sc.textFile("hdfs://localhost:9000/data/esempio.txt")

    val pairs = file.flatMap(hobby2name).reduceByKey(_ ::: _)
    val namestopairs = pairs.flatMap(pair =>{
      pair._2.combinations(2)
    }).map(l=> l(0)+" , "+l(1))

    namestopairs.saveAsTextFile("hdfs://localhost:9000/data/output/exercise2/")


  }

  def hobby2name(line:String) : List[Tuple2[String,List[String]]] = {
    val name::hobbies = line.split(" ").toList
    hobbies.map(hobby => (hobby, List(name)))
  }

}
