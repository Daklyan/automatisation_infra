package esgi.circulation

import esgi.circulation.Clean.date_column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, date_format}


object Jointure {

  var joinColumn = "iu_ac"
  var countColumn = "iu_nd_amont"
  var countOverColumn = "iu_ac"

  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("Joint-grp5")
      .getOrCreate()

    val inputFile = args(0)
    val joinFile = args(1)
    val outputFile = args(2)

    print("inputFile: " + inputFile)
    print("joinFile:" + joinFile)
    print("outputFile:" + outputFile)

    // TODO : lire son fichier d'input et son fichier de jointure
    var df = spark.read.csv(inputFile)
    val joinDf = spark.read.parquet(joinFile)

    // TODO : ajouter ses transformations Spark avec au minimum une jointure et une agrégation
    df = df.as("df").join(joinDf.as("joinDf"), df(joinColumn) === joinDf("iu_ac")).select("df.*", "joinDf.trust")
    df = df.withColumn("count", functions.count(countColumn).over(Window.partitionBy(countOverColumn))).orderBy("count")

    // TODO : écrire le résultat dans un format pratique pour la dataviz
    df.write.csv(outputFile)
  }
}