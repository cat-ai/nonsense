package io.cat.ai.nonsense.app.free

import cats.effect.{Async, ExitCase}
import cats.free.Free
import cats.{Monad, ~>}

import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}

trait DataFrameSaverAlgebra[P] {
  def service[F[_]](v: DataFrameSaverAlgebra.Service[F]): F[P]
}

object DataFrameSaverAlgebra { base =>

  type DataFrameSaverIO[P]  = Free[DataFrameSaverAlgebra, P]

  trait Service[F[_]] extends (DataFrameSaverAlgebra ~> F) { self =>

    final def apply[P](fa: DataFrameSaverAlgebra[P]): F[P] =
      fa service self

    def raw[A](f: DataFrame => A): F[A]
    def embed[A](e: Embedded[A]): F[A]
    def delay[A](a: () => A): F[A]
    def handleErrorWith[A](fa: DataFrameSaverIO[A],
                           f: Throwable => DataFrameSaverIO[A]): F[A]
    def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
    def asyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameSaverIO[Unit]): F[A]
    def bracketCase[A, B](acquire: DataFrameSaverIO[A])
                         (use: A => DataFrameSaverIO[B])
                         (release: (A, ExitCase[Throwable]) => DataFrameSaverIO[Unit]): F[B]
    def raiseError[A](e: Throwable): F[A]
    def shift: F[Unit]
    def evalOn[A](ec: ExecutionContext)
                 (fa: DataFrameSaverIO[A]): F[A]

    def saveParquet(path: String,
                    options: Map[String, String]): F[Unit]
    def saveOrc(path: String,
                options: Map[String, String]): F[Unit]

    def saveAvro(path: String,
                 options: Map[String, String]): F[Unit]

    def saveCsv(path: String,
                options: Map[String, String]): F[Unit]

    def saveElastic(options: Map[String, String]): F[Unit]

    def saveHive(table: String): F[Unit]
  }

  final case class Raw[A](f: DataFrame => A) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service raw f
  }

  final case class Embed[A](embeddedA: Embedded[A]) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service embed embeddedA
  }

  final case class Delay[A](a: () => A) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service delay a
  }

  final case class HandleErrorWith[A](fa: DataFrameSaverIO[A], f: Throwable => DataFrameSaverIO[A]) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service handleErrorWith(fa, f)
  }

  final case class RaiseError[A](e: Throwable) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service raiseError[A] e
  }

  final case object Shift extends DataFrameSaverAlgebra[Unit] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[Unit] =
      service shift
  }

  final case class EvalOn[A](ec: ExecutionContext,
                             fa: DataFrameSaverIO[A]) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service.evalOn(ec)(fa)
  }

  final case class AsyncA[A](k: (Either[Throwable, A] => Unit) => Unit) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service async k
  }

  final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameSaverIO[Unit]) extends DataFrameSaverAlgebra[A] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[A] =
      service asyncF k
  }

  final case class BracketCase[A, B](acquire: DataFrameSaverIO[A],
                                     use: A => DataFrameSaverIO[B],
                                     release: (A, ExitCase[Throwable]) => DataFrameSaverIO[Unit]) extends DataFrameSaverAlgebra[B] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[B] =
      service.bracketCase(acquire)(use)(release)
  }

  final case class SaveParquet(path: String,
                               options: Map[String, String]) extends DataFrameSaverAlgebra[Unit] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[Unit] =
      service saveParquet(path, options)
  }

  final case class SaveOrc(path: String,
                           options: Map[String, String]) extends DataFrameSaverAlgebra[Unit] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[Unit] =
      service saveOrc(path, options)
  }

  final case class SaveAvro(path: String,
                            options: Map[String, String]) extends DataFrameSaverAlgebra[Unit] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[Unit] =
      service saveAvro(path, options)
  }

  final case class SaveCsv(path: String,
                           options: Map[String, String]) extends DataFrameSaverAlgebra[Unit] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[Unit] =
      service saveCsv(path, options)
  }

  final case class SaveElastic(options: Map[String, String]) extends DataFrameSaverAlgebra[Unit] {
    def service[F[_]](service: DataFrameSaverAlgebra.Service[F]): F[Unit] =
      service saveElastic options
  }

  def raw[A](f: DataFrame => A): Free[Raw, A] =
    Free liftF Raw(f)

  def embed[F[_], E, A](e: E, fa: Free[F, A])
                       (implicit EV: Embeddable[F, E]): Free[Embed, A] =
    Free liftF Embed(EV embedded(e, fa))

  def delay[A](a: => A): DataFrameSaverIO[A] =
    Free liftF[DataFrameSaverAlgebra, A] Delay(() => a)

  def handleErrorWith[A](fa: DataFrameSaverIO[A],
                         f: Throwable => DataFrameSaverIO[A]): DataFrameSaverIO[A] =
    Free liftF[DataFrameSaverAlgebra, A] HandleErrorWith(fa, f)

  def raiseError[S](err: Throwable): DataFrameSaverIO[S] =
    Free liftF[DataFrameSaverAlgebra, S] RaiseError(err)

  def evalOn[A](ec: ExecutionContext)(fa: DataFrameSaverIO[A]): Free[DataFrameSaverAlgebra, A] =
    Free liftF[DataFrameSaverAlgebra, A] EvalOn(ec, fa)

  def async[A](k: (Either[Throwable, A] => Unit) => Unit): DataFrameSaverIO[A] =
    Free liftF[DataFrameSaverAlgebra, A] AsyncA(k)

  def asyncF[A](k: (Either[Throwable, A] => Unit) => DataFrameSaverIO[Unit]): DataFrameSaverIO[A] =
    Free liftF[DataFrameSaverAlgebra, A] AsyncF(k)

  def bracketCase[A, B](acquire: DataFrameSaverIO[A])
                       (use: A => DataFrameSaverIO[B])
                       (release: (A, ExitCase[Throwable]) => DataFrameSaverIO[Unit]): DataFrameSaverIO[B] =
    Free liftF[DataFrameSaverAlgebra, B] BracketCase(acquire, use, release)

  val shift: DataFrameSaverIO[Unit] =
    Free liftF[DataFrameSaverAlgebra, Unit] Shift

  def saveParquet(path: String,
                  options: Map[String, String] = Map.empty[String, String]): Free[DataFrameSaverAlgebra, Unit] =
    Free liftF[DataFrameSaverAlgebra, Unit] SaveParquet(path, options)

  def saveOrc(path: String,
              options: Map[String, String] = Map.empty[String, String]): Free[DataFrameSaverAlgebra, Unit] =
    Free liftF[DataFrameSaverAlgebra, Unit] SaveOrc(path, options)

  def saveAvro(path: String,
               options: Map[String, String] = Map.empty[String, String]): Free[DataFrameSaverAlgebra, Unit] =
    Free liftF[DataFrameSaverAlgebra, Unit] SaveAvro(path, options)

  def saveCsv(path: String,
              options: Map[String, String] = Map.empty[String, String]): Free[DataFrameSaverAlgebra, Unit] =
    Free liftF[DataFrameSaverAlgebra, Unit] SaveCsv(path, options)

  def saveElastic(options: Map[String, String]): Free[DataFrameSaverAlgebra, Unit] =
    Free liftF[DataFrameSaverAlgebra, Unit] SaveElastic(options)

  trait Implicits {

    implicit val DataFrameSaverAlgebraEmbeddable: Embeddable[DataFrameSaverAlgebra, DataFrame] =
      new Embeddable[DataFrameSaverAlgebra, DataFrame] {
        def embedded[A](df: DataFrame,
                        fa: Free[DataFrameSaverAlgebra, A]): Embedded.DataFrameSaver[A] =
          Embedded.DataFrameSaver(df, fa)
      }

    implicit val AsyncDataFrameSaverIO: Async[DataFrameSaverIO] =
      new Async[DataFrameSaverIO] {

        val asyncM: Monad[Free[DataFrameSaverAlgebra, *]] = Free.catsFreeMonadForFree[DataFrameSaverAlgebra]

        def bracketCase[A, B](acquire: DataFrameSaverIO[A])
                             (use: A => DataFrameSaverIO[B])
                             (release: (A, ExitCase[Throwable]) => DataFrameSaverIO[Unit]): DataFrameSaverIO[B] =
          base.bracketCase(acquire)(use)(release)

        def pure[A](x: A): DataFrameSaverIO[A] =
          asyncM pure x

        def handleErrorWith[A](fa: DataFrameSaverIO[A])
                              (f: Throwable => DataFrameSaverIO[A]): DataFrameSaverIO[A] =
          base handleErrorWith(fa, f)

        def raiseError[A](e: Throwable): DataFrameSaverIO[A] =
          base raiseError[A] e

        def async[A](k: (Either[Throwable,A] => Unit) => Unit): DataFrameSaverIO[A] =
          base async k

        def asyncF[A](k: (Either[Throwable,A] => Unit) => DataFrameSaverIO[Unit]): DataFrameSaverIO[A] =
          base asyncF k

        def flatMap[A, B](fa: DataFrameSaverIO[A])
                         (f: A => DataFrameSaverIO[B]): DataFrameSaverIO[B] =
          asyncM.flatMap(fa)(f)

        def tailRecM[A, B](a: A)
                          (f: A => DataFrameSaverIO[Either[A, B]]): DataFrameSaverIO[B] =
          asyncM.tailRecM(a)(f)

        def suspend[A](thunk: => DataFrameSaverIO[A]): DataFrameSaverIO[A] =
          asyncM flatten(base delay thunk)
      }
  }
}