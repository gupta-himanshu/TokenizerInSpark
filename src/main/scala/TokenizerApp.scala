import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TokenizerApp {
  def main(args: Array[String]) {
    val logFile = "src/data/sample.txt" // Should be some file on your system
    val sc = new SparkContext("local", "Tokenizer App", "/path/to/spark-0.9.1-incubating",
      List("target/scala-2.10/simple-project_2.10-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val tokens = sc.textFile(logFile, 2).flatMap(line => line.split(" "))
    val termFrequency = tokens.map(word => (word, 1)).reduceByKey((a, b) => a + b)
    termFrequency.collect.map(tf => println("Term, Frequency: " + tf))
    tokens.saveAsTextFile("src/data/tokens")
    termFrequency.saveAsTextFile("src/data/term_frequency")
  }
}
