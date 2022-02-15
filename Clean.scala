package esgi.circulation

import org.apache.spark.sql.functions.{col, date_format}


object Clean {
  var date_column = "Date et heure de comptage"

  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("Clean")
      .getOrCreate()

    val inputFile = args(0)
    val outputFile = args(1)

    print("inputFile: " + inputFile)
    print("outputFile:" + outputFile)

    // TODO : lire son fichier d'input
    val df = spark.read.csv(inputFile)

    // TODO : ajouter 3 colonnes à votre dataframe pour l'année, le mois et le jour
    df.withColumn("day", date_format(col(date_column),"dd MM YYYY HH:mm").as("dd"))
    df.withColumn("month", date_format(col(date_column),"dd MM YYYY HH:mm").as("MM"))
    df.withColumn("year", date_format(col(date_column),"dd MM YYYY HH:mm").as("YYYY"))

    // TODO : écrire le fichier en parquet et partitionné par année / mois / jour
    df.write.partitionBy("year", "month", "day").parquet(outputFile)
  }
}