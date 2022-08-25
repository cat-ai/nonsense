package io.cat.ai.nonsense.app.free

import cats.effect.{Async, ExitCase}
import cats.free.Free
import cats.{Monad, ~>}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}

trait DataFrameLoaderAlgebra[P] {
  def service[F[_]](v: DataFrameLoaderAlgebra.Service[F]): F[P]
}

object DataFrameLoaderAlgebra { base =>

  type DataFrameLoaderIO[S] = Free[DataFrameLoaderAlgebra, S]

  trait Service[F[_]] extends (DataFrameLoaderAlgebra ~> F) { self =>

    final def apply[P](fa: DataFrameLoaderAlgebra[P]): F[P] =
      fa service self

    def raw[A](f: SparkSession => A): F[A]
    def embed[A](e: Embedded[A]): F[A]
    def delay[A](a: () => A): F[A]
    def handleErrorWith[A](fa: DataFrameLoaderIO[A],
                           f: Throwable => DataFrameLoaderIO[A]): F[A]
    def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
    def asyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameLoaderIO[Unit]): F[A]
    def raiseError[A](e: Throwable): F[A]
    def shift: F[Unit]
    def evalOn[A](ec: ExecutionContext)
                 (fa: DataFrameLoaderIO[A]): F[A]
    def bracketCase[A, B](acquire: DataFrameLoaderIO[A])
                         (use: A => DataFrameLoaderIO[B])
                         (release: (A, ExitCase[Throwable]) => DataFrameLoaderIO[Unit]): F[B]

    def loadParquet(path: Seq[String],
                    schema: Option[StructType],
                    options: Map[String, String]): F[DataFrame]

    def loadOrc(path: Seq[String],
                schema: Option[StructType],
                options: Map[String, String]): F[DataFrame]

    def loadAvro(path: Seq[String],
                 schema: Option[StructType],
                 options: Map[String, String]): F[DataFrame]

    def loadCsv(path: Seq[String],
                schema: Option[StructType],
                options: Map[String, String]): F[DataFrame]

    def loadElastic(opts: Map[String, String]): F[DataFrame]

    def loadHive(table: String): F[DataFrame]
  }

  final case class Raw[A](f: SparkSession => A) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service raw f
  }

  final case class Embed[A](embeddedA: Embedded[A]) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service embed embeddedA
  }

  final case class Delay[A](a: () => A) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service delay a
  }

  final case class HandleErrorWith[A](fa: DataFrameLoaderIO[A],
                                      f: Throwable => DataFrameLoaderIO[A]) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service handleErrorWith(fa, f)
  }

  final case class RaiseError[A](e: Throwable) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service raiseError[A] e
  }

  final case class AsyncA[A](k: (Either[Throwable, A] => Unit) => Unit) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service async k
  }

  final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameLoaderIO[Unit]) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service asyncF k
  }

  final case class BracketCase[A, B](acquire: DataFrameLoaderIO[A],
                                     use: A => DataFrameLoaderIO[B],
                                     release: (A, ExitCase[Throwable]) => DataFrameLoaderIO[Unit]) extends DataFrameLoaderAlgebra[B] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[B] =
      service.bracketCase(acquire)(use)(release)
  }

  final case object Shift extends DataFrameLoaderAlgebra[Unit] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[Unit] =
      service shift
  }

  final case class EvalOn[A](ec: ExecutionContext,
                             fa: DataFrameLoaderIO[A]) extends DataFrameLoaderAlgebra[A] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[A] =
      service.evalOn(ec)(fa)
  }

  final case class LoadParquet(paths: Seq[String],
                               schema: Option[StructType],
                               options: Map[String, String]) extends DataFrameLoaderAlgebra[DataFrame] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[DataFrame] =
      service loadParquet(paths, schema, options)
  }

  final case class LoadOrc(paths: Seq[String],
                           schema: Option[StructType],
                           options: Map[String, String]) extends DataFrameLoaderAlgebra[DataFrame] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[DataFrame] =
      service loadOrc(paths, schema, options)
  }

  final case class LoadAvro(paths: Seq[String],
                            schema: Option[StructType],
                            options: Map[String, String]) extends DataFrameLoaderAlgebra[DataFrame] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[DataFrame] =
      service loadAvro(paths, schema, options)
  }

  final case class LoadCsv(paths: Seq[String],
                           schema: Option[StructType],
                           options: Map[String, String]) extends DataFrameLoaderAlgebra[DataFrame] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[DataFrame] =
      service loadCsv(paths, schema, options)
  }

  final case class LoadElastic(options: Map[String, String]) extends DataFrameLoaderAlgebra[DataFrame] {
    def service[F[_]](service: DataFrameLoaderAlgebra.Service[F]): F[DataFrame] =
      service loadElastic options
  }

  val unit: Free[DataFrameLoaderAlgebra, Unit] =
    Free pure[DataFrameLoaderAlgebra, Unit] ()

  def pure[A](a: A):  Free[DataFrameLoaderAlgebra, A] =
    Free pure[DataFrameLoaderAlgebra, A] a

  def raw[A](f: SparkSession => A): Free[Raw, A] =
    Free liftF Raw(f)

  def embed[F[_], E, A](e: E, fa: Free[F, A])(implicit EV: Embeddable[F, E]): Free[Embed, A] =
    Free liftF Embed(EV embedded(e, fa))

  def delay[A](a: => A): DataFrameLoaderIO[A] =
    Free liftF[DataFrameLoaderAlgebra, A] Delay(() => a)

  def handleErrorWith[A](fa: DataFrameLoaderIO[A],
                         f: Throwable => DataFrameLoaderIO[A]): DataFrameLoaderIO[A] =
    Free liftF[DataFrameLoaderAlgebra, A] HandleErrorWith(fa, f)

  def async[A](k: (Either[Throwable, A] => Unit) => Unit): DataFrameLoaderIO[A] =
    Free liftF[DataFrameLoaderAlgebra, A] AsyncA(k)

  def asyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameLoaderIO[Unit]): DataFrameLoaderIO[A] =
    Free liftF[DataFrameLoaderAlgebra, A] AsyncF(k)

  def bracketCase[A, B](acquire: DataFrameLoaderIO[A])
                       (use: A => DataFrameLoaderIO[B])
                       (release: (A, ExitCase[Throwable]) => DataFrameLoaderIO[Unit]): DataFrameLoaderIO[B] =
    Free liftF[DataFrameLoaderAlgebra, B] BracketCase(acquire, use, release)

  def raiseError[S](err: Throwable): DataFrameLoaderIO[S] =
    Free liftF[DataFrameLoaderAlgebra, S] RaiseError(err)

  def evalOn[A](ec: ExecutionContext)
               (fa: DataFrameLoaderIO[A]): Free[DataFrameLoaderAlgebra, A] =
    Free liftF[DataFrameLoaderAlgebra, A] EvalOn(ec, fa)

  val shift: DataFrameLoaderIO[Unit] =
    Free liftF[DataFrameLoaderAlgebra, Unit] Shift

  def loadParquet(paths: Seq[String],
                  schema: Option[StructType] = None,
                  options: Map[String, String] = Map.empty[String, String]): Free[DataFrameLoaderAlgebra, DataFrame] =
    Free liftF[DataFrameLoaderAlgebra, DataFrame] LoadParquet(paths, schema, options)

  def loadOrc(paths: Seq[String],
              schema: Option[StructType] = None,
              options: Map[String, String] = Map.empty[String, String]): Free[DataFrameLoaderAlgebra, DataFrame] =
    Free liftF[DataFrameLoaderAlgebra, DataFrame] LoadOrc(paths, schema, options)

  def loadAvro(paths: Seq[String],
               schema: Option[StructType] = None,
               options: Map[String, String] = Map.empty[String, String]): Free[DataFrameLoaderAlgebra, DataFrame] =
    Free liftF[DataFrameLoaderAlgebra, DataFrame] LoadAvro(paths, schema, options)

  def loadCsv(paths: Seq[String],
              schema: Option[StructType] = None,
              options: Map[String, String] = Map.empty[String, String]): Free[DataFrameLoaderAlgebra, DataFrame] =
    Free liftF[DataFrameLoaderAlgebra, DataFrame] LoadCsv(paths, schema, options)

  def loadElastic(options: Map[String, String] = Map.empty[String, String]): Free[DataFrameLoaderAlgebra, DataFrame] =
    Free liftF[DataFrameLoaderAlgebra, DataFrame] LoadElastic(options)

  trait Implicits {

    implicit val DataFrameLoaderAlgebraEmbeddable: Embeddable[DataFrameLoaderAlgebra, SparkSession] =
      new Embeddable[DataFrameLoaderAlgebra, SparkSession] {
        def embedded[A](j: SparkSession,
                        fa: Free[DataFrameLoaderAlgebra, A]): Embedded.DataFrameLoader[A] =
          Embedded.DataFrameLoader(j, fa)
      }

    implicit val AsyncDataFrameLoaderIO: Async[DataFrameLoaderIO] =
      new Async[DataFrameLoaderIO] {

        val asyncM: Monad[Free[DataFrameLoaderAlgebra, *]] = Free.catsFreeMonadForFree[DataFrameLoaderAlgebra]

        def bracketCase[A, B](acquire: DataFrameLoaderIO[A])
                             (use: A => DataFrameLoaderIO[B])
                             (release: (A, ExitCase[Throwable]) => DataFrameLoaderIO[Unit]): DataFrameLoaderIO[B] =
          base.bracketCase(acquire)(use)(release)

        def pure[A](x: A): DataFrameLoaderIO[A] =
          asyncM pure x

        def handleErrorWith[A](fa: DataFrameLoaderIO[A])
                              (f: Throwable => DataFrameLoaderIO[A]): DataFrameLoaderIO[A] =
          base handleErrorWith(fa, f)

        def raiseError[A](e: Throwable): DataFrameLoaderIO[A] =
          base raiseError[A] e

        def async[A](k: (Either[Throwable,A] => Unit) => Unit): DataFrameLoaderIO[A] =
          base async k

        def asyncF[A](k: (Either[Throwable,A] => Unit) => DataFrameLoaderIO[Unit]): DataFrameLoaderIO[A] =
          base asyncF k

        def flatMap[A, B](fa: DataFrameLoaderIO[A])
                         (f: A => DataFrameLoaderIO[B]): DataFrameLoaderIO[B] =
          asyncM.flatMap(fa)(f)

        def tailRecM[A, B](a: A)
                          (f: A => DataFrameLoaderIO[Either[A, B]]): DataFrameLoaderIO[B] =
          asyncM.tailRecM(a)(f)

        def suspend[A](thunk: => DataFrameLoaderIO[A]): DataFrameLoaderIO[A] =
          asyncM flatten(base delay thunk)
      }
  }
}