package SparkTest005.Project1Spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Exercise2 {
  def main(args: Array[String]) {

    val sc = new SparkContext("spark://ec2-54-185-216-73.us-west-2.compute.amazonaws.com:7077", "Ex3",
      System.getenv("SPARK_HOME"), Seq("s3n://littledatabucket/jars/SparkTest005.jar"))


    val file = sc.textFile("s3n://littledatabucket/input/esempio5k.txt")

    val pairs = file.flatMap(hobby2name).reduceByKey(_ ::: _)
    val result = pairs.flatMap(pair => names2pairs(pair._2)).reduceByKey(_ + _).map(_._1)

    result.saveAsTextFile("s3n://littledatabucket/output/es2/")


  }

  def hobby2name(line:String) : List[Tuple2[String,List[String]]] = {
    val name::hobbies = line.split(" ").toList
    hobbies.map(hobby => (hobby, List(name)))
  }

  def names2pairs(names: List[String]): List[Tuple2[String, Int]] = {
    names.combinations(2).toList.map(couple => ("%s,%s".format(couple(0), couple(1)), 0))
  }
}
