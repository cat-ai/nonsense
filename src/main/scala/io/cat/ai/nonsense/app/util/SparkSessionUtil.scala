package io.cat.ai.nonsense.app.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUtil {

  def spark(sparkConf: SparkConf): SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate

}
