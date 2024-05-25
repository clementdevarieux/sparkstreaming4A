package mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count


object Producer {
  def main(args: Array[String]): Unit = {

    val logFile = "eco2mix-regional-tr.csv" // Chemin du fichier CSV

    val spark = SparkSession.builder
      .appName("Producer")
      .master("local[*]") // Ajoutez ceci pour exécuter localement avec tous les cœurs
      .getOrCreate()

    val options = Map("delimiter"->";","header"->"true")
    // Lecture du fichier CSV et création d'un DataFrame
    var logData = spark.read.options(options).csv(logFile)

    // val amount_per_day = logData.groupBy(col("Date")).count()

    val number_of_partitions = logData.count()/1152
    // println(number_of_partitions)
    for (i <- 0 to number_of_partitions.toInt) {
      val to_write = logData.limit(1152)
      // Écrire le DataFrame actuel au format CSV
      to_write.write
        .format("csv")
        .options(options)
        .mode("overwrite")
        .save(s"data/partition_${i}.csv")

      // Supprimer les 10000 premières lignes du DataFrame
      logData = logData.except(to_write)
      // Thread.sleep(5000)
    }

    spark.stop()
  }
}

