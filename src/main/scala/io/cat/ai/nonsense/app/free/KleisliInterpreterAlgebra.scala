package io.cat.ai.nonsense.app.free

import cats.data.Kleisli
import cats.effect.{Async, Blocker, ContextShift, ExitCase}
import cats.~>

import io.cat.ai.nonsense.app.free.DataFrameLoaderAlgebra._
import io.cat.ai.nonsense.app.free.DataFrameSaverAlgebra._
import io.cat.ai.nonsense.app.free.SparkSessionAlgebra._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}
import scala.util.{Failure, Success, Try}

trait KleisliInterpreterAlgebra[M[_]] { base =>

  implicit def asyncM: Async[M]

  def contextShiftM: ContextShift[M]
  def blocker: Blocker

  lazy val SparkSessionInterpreter:    SparkSessionAlgebra    ~> Kleisli[M, SparkSession, *] = new SparkSessionInterpreter {}
  lazy val DataFrameLoaderInterpreter: DataFrameLoaderAlgebra ~> Kleisli[M, SparkSession, *] = new DataFrameLoaderInterpreter {}
  lazy val DataFrameSaverInterpreter:  DataFrameSaverAlgebra  ~> Kleisli[M, DataFrame,    *] = new DataFrameSaverInterpreter {}

  def primitive[A, B](f: A => B): Kleisli[M, A, B] =
    Kleisli { a =>
      blocker.blockOn[M, B] {
        Try(asyncM delay f(a)) match {
          case Failure(e)     => asyncM raiseError[B] e
          case Success(value) => value
        }
      }(contextShiftM)
  }

  def delay[S, A](a: () => A): Kleisli[M, S, A] =
    Kleisli(_ => asyncM delay a())

  def raw[S, A](f: S => A): Kleisli[M, S, A] =
    primitive(f)

  def raiseError[S, A](e: Throwable): Kleisli[M, S, A] =
    Kleisli(_ => asyncM raiseError[A] e)

  def embedded[S, A](e: Embedded[A]): Kleisli[M, S, A] =
    e match {
      case Embedded.SparkSession(spark, fa)    => Kleisli(_ => fa foldMap SparkSessionInterpreter    run spark)
      case Embedded.DataFrameLoader(spark, fa) => Kleisli(_ => fa foldMap DataFrameLoaderInterpreter run spark)
      case Embedded.DataFrameSaver(df, fa)     => Kleisli(_ => fa foldMap DataFrameSaverInterpreter  run df)
    }

  def async[S, A](k: (Either[Throwable, A] => Unit) => Unit): Kleisli[M, S, A] =
    Kleisli(_ => asyncM async k)

  trait SparkSessionInterpreter extends SparkSessionAlgebra.Service[Kleisli[M, SparkSession, *]] { self =>

    override def raw[A](f: SparkSession => A): Kleisli[M, SparkSession, A] =
      base raw f

    override def embed[A](e: Embedded[A]): Kleisli[M, SparkSession, A] =
      base embedded e

    override def delay[A](a: () => A): Kleisli[M, SparkSession, A] =
      base delay a

    override def raiseError[A](err: Throwable): Kleisli[M, SparkSession, A] =
      base raiseError[SparkSession, A] err

    override val shift: Kleisli[M, SparkSession, Unit] =
      Kleisli(_ => contextShiftM shift)

    override def async[A](k: (Either[Throwable, A] => Unit) => Unit): Kleisli[M, SparkSession, A] =
      Kleisli(_ => asyncM async k)

    override def asyncF[A](k: (Either[Throwable, A] => Unit) => SparkSessionIO[Unit]): Kleisli[M, SparkSession, A] =
      Kleisli(spark => asyncM asyncF(k andThen(_ foldMap self run spark)))

    override def handleErrorWith[A](fa: SparkSessionIO[A],
                                    f: Throwable => SparkSessionIO[A]): Kleisli[M, SparkSession, A] =
      Kleisli { spark =>
        val faʹ = fa foldMap self run spark
        val fʹ  = f andThen(_ foldMap self run spark)
        asyncM.handleErrorWith(faʹ)(fʹ)
      }

    override def evalOn[A](ec: ExecutionContext)
                          (fa: SparkSessionIO[A]): Kleisli[M, SparkSession, A] =
      Kleisli(spark => contextShiftM.evalOn(ec)(fa foldMap self run spark))

    def bracketCase[A, B](acquire: SparkSessionIO[A])
                         (use: A => SparkSessionIO[B])
                         (release: (A, ExitCase[Throwable]) => SparkSessionIO[Unit]): Kleisli[M, SparkSession, B] =
      Kleisli { spark =>
          asyncM.bracketCase(acquire foldMap self run spark)(use andThen(_ foldMap self run spark))((a, e) => release(a, e) foldMap self run spark)
      }

    override def newSession: Kleisli[M, SparkSession, SparkSession] =
      primitive(_ newSession)

    override def emptyDataFrame: Kleisli[M, SparkSession, DataFrame] =
      primitive(_ emptyDataFrame)

    override def close: Kleisli[M, SparkSession, Unit] =
      primitive(_ close)
  }

  trait DataFrameLoaderInterpreter extends DataFrameLoaderAlgebra.Service[Kleisli[M, SparkSession, *]] { self =>

    override def raw[A](f: SparkSession => A): Kleisli[M, SparkSession, A] =
      base raw f

    override def embed[A](e: Embedded[A]): Kleisli[M, SparkSession, A] =
      base embedded e

    override def delay[A](a: () => A): Kleisli[M, SparkSession, A] =
      base delay a

    override def raiseError[A](err: Throwable): Kleisli[M, SparkSession, A] =
      base raiseError[SparkSession, A] err

    override def async[A](k: (Either[Throwable, A] => Unit) => Unit): Kleisli[M, SparkSession, A] =
      Kleisli(_ => asyncM async k)

    override def asyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameLoaderIO[Unit]): Kleisli[M, SparkSession, A] =
      Kleisli(spark => asyncM asyncF(k andThen(_ foldMap self run spark)))

    override def bracketCase[A, B](acquire: DataFrameLoaderIO[A])
                                  (use: A => DataFrameLoaderIO[B])
                                  (release: (A, ExitCase[Throwable]) => DataFrameLoaderIO[Unit]): Kleisli[M, SparkSession, B] =
      Kleisli {
        spark =>
          asyncM.bracketCase(acquire foldMap self run spark)(use andThen(_ foldMap self run spark))((a, e) => release(a, e) foldMap self run spark)
      }

    val shift: Kleisli[M, SparkSession, Unit] =
      Kleisli(_ => contextShiftM shift)

    override def handleErrorWith[A](fa: DataFrameLoaderIO[A],
                                    f: Throwable => DataFrameLoaderIO[A]): Kleisli[M, SparkSession, A] =
      Kleisli { spark =>
        val faʹ = fa foldMap self run spark
        val fʹ  = f andThen(_ foldMap self run spark)
        asyncM.handleErrorWith(faʹ)(fʹ)
      }

    override def evalOn[A](ec: ExecutionContext)
                          (fa: DataFrameLoaderIO[A]): Kleisli[M, SparkSession, A] =
      Kleisli(spark => contextShiftM.evalOn(ec)(fa foldMap self run spark))

    private[this] def load(fmt: String,
                           paths: Seq[String],
                           schema: Option[StructType],
                           options: Map[String, String]): Kleisli[M, SparkSession, DataFrame] =
      schema -> options match {
        case (None,      opts: Map[String, String]) if opts isEmpty  => primitive(_.read format fmt load(paths:_*))
        case (Some(sch), opts: Map[String, String]) if opts isEmpty  => primitive(_.read format fmt schema sch load(paths:_*))
        case (None,      opts: Map[String, String]) if opts nonEmpty => primitive(_.read format fmt options opts load(paths:_*))
        case (Some(sch), opts: Map[String, String]) if opts nonEmpty => primitive(_.read format fmt schema sch options opts load(paths:_*))
      }

    override def loadParquet(paths: Seq[String],
                             schema: Option[StructType] = None,
                             options: Map[String, String] = Map.empty[String, String]): Kleisli[M, SparkSession, DataFrame] =
      load("parquet", paths, schema, options)

    override def loadOrc(paths: Seq[String],
                         schema: Option[StructType] = None,
                         options: Map[String, String] = Map.empty[String, String]): Kleisli[M, SparkSession, DataFrame] =
      load("orc", paths, schema, options)

    override def loadAvro(paths: Seq[String],
                          schema: Option[StructType] = None,
                          options: Map[String, String] = Map.empty[String, String]): Kleisli[M, SparkSession, DataFrame] =
      load("avro", paths, schema, options)

    override def loadCsv(paths: Seq[String],
                         schema: Option[StructType] = None,
                         options: Map[String, String] = Map.empty[String, String]): Kleisli[M, SparkSession, DataFrame] =
      load("csv", paths, schema, options)

    override def loadElastic(opts: Map[String, String] = Map.empty[String, String]): Kleisli[M, SparkSession, DataFrame] =
      primitive(_.read format "org.elasticsearch.spark.sql" options opts load)

    override def loadHive(hiveTable: String): Kleisli[M, SparkSession, DataFrame] =
      primitive(_.read format "hive" table hiveTable)
  }

  trait DataFrameSaverInterpreter extends DataFrameSaverAlgebra.Service[Kleisli[M, DataFrame, *]] { self =>

    override def raw[A](f: DataFrame => A): Kleisli[M, DataFrame, A] =
      base raw f

    override def embed[A](e: Embedded[A]): Kleisli[M, DataFrame, A] =
      base embedded e

    override def delay[A](a: () => A): Kleisli[M, DataFrame, A] =
      base delay a

    override def async[A](k: (Either[Throwable, A] => Unit) => Unit): Kleisli[M, DataFrame, A] =
      Kleisli(_ => asyncM async k)

    override def asyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameSaverIO[Unit]): Kleisli[M, DataFrame, A] =
      Kleisli(dataFrame => asyncM asyncF(k andThen(_ foldMap self run dataFrame)))

    override def bracketCase[A, B](acquire: DataFrameSaverIO[A])
                                  (use: A => DataFrameSaverIO[B])
                                  (release: (A, ExitCase[Throwable]) => DataFrameSaverIO[Unit]): Kleisli[M, DataFrame, B] =
      Kleisli {
        dataFrame =>
          asyncM.bracketCase(acquire foldMap self run dataFrame)(use andThen(_ foldMap self run dataFrame))((a, e) => release(a, e) foldMap self run dataFrame)
      }

    override def raiseError[A](err: Throwable): Kleisli[M, DataFrame, A] =
      base raiseError[DataFrame, A] err

    val shift: Kleisli[M, DataFrame, Unit] =
      Kleisli(_ => contextShiftM shift)

    override def handleErrorWith[A](fa: DataFrameSaverIO[A],
                                    f: Throwable => DataFrameSaverIO[A]): Kleisli[M, DataFrame, A] =
      Kleisli { spark =>
        val faʹ = fa foldMap self run spark
        val fʹ  = f andThen(_ foldMap self run spark)
        asyncM.handleErrorWith(faʹ)(fʹ)
      }

    override def evalOn[A](ec: ExecutionContext)
                          (fa: DataFrameSaverIO[A]): Kleisli[M, DataFrame, A] =
      Kleisli(spark => contextShiftM.evalOn(ec)(fa foldMap self run spark))

    private[this] def save(fmt: String,
                           path: String,
                           options: Map[String, String]): Kleisli[M, DataFrame, Unit] =
      options match {
        case map: Map[String, String] if map isEmpty  => primitive(_.write format fmt save path)
        case map: Map[String, String] if map nonEmpty => primitive(_.write format fmt options map save path)
      }

    override def saveParquet(path: String,
                             options: Map[String, String] = Map.empty[String, String]): Kleisli[M, DataFrame, Unit] =
      save("parquet", path, options)

    override def saveOrc(path: String,
                         options: Map[String, String] = Map.empty[String, String]): Kleisli[M, DataFrame, Unit] =
      save("orc", path, options)

    override def saveAvro(path: String,
                          options: Map[String, String] = Map.empty[String, String]): Kleisli[M, DataFrame, Unit] =
      save("avro", path, options)

    override def saveCsv(path: String,
                         options: Map[String, String] = Map.empty[String, String]): Kleisli[M, DataFrame, Unit] =
      save("csv", path, options)

    override def saveElastic(options: Map[String, String] = Map.empty[String, String]): Kleisli[M, DataFrame, Unit] =
      ???

    override def saveHive(hiveTable: String): Kleisli[M, DataFrame, Unit] =
      primitive(_.write format "hive" saveAsTable hiveTable)
  }
}

object KleisliInterpreterAlgebra {

  object Implicits {
    object SparkSessionImplicits    extends SparkSessionAlgebra.Implicits
    object DataFrameLoaderImplicits extends DataFrameLoaderAlgebra.Implicits
    object DataFrameSaverImplicits  extends DataFrameSaverAlgebra.Implicits
  }

  def apply[M[_]](B: Blocker)
                 (implicit CS: ContextShift[M], AsyncM: Async[M]): KleisliInterpreterAlgebra[M] =
    new KleisliInterpreterAlgebra[M] {
      def asyncM: Async[M] = AsyncM
      def contextShiftM: ContextShift[M] = CS
      def blocker: Blocker = B
    }
}