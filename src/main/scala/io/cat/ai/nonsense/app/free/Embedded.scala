package io.cat.ai.nonsense.app.free

import cats.free.Free

import io.cat.ai.nonsense.app.free.DataFrameLoaderAlgebra.DataFrameLoaderIO
import io.cat.ai.nonsense.app.free.DataFrameSaverAlgebra.DataFrameSaverIO
import io.cat.ai.nonsense.app.free.SparkSessionAlgebra.SparkSessionIO

import org.apache.spark.sql.{DataFrame, SparkSession => ApacheSparkSession}

import scala.language.higherKinds

sealed trait Embedded[A]

object Embedded {
  final case class SparkSession[A]   (spark: ApacheSparkSession, fa: SparkSessionIO[A])    extends Embedded[A]
  final case class DataFrameLoader[A](spark: ApacheSparkSession, fa: DataFrameLoaderIO[A]) extends Embedded[A]
  final case class DataFrameSaver[A] (dataFrame: DataFrame,      fa: DataFrameSaverIO[A])  extends Embedded[A]
}

trait Embeddable[F[_], S] {
  def embedded[A](s: S, fa: Free[F, A]): Embedded[A]
}