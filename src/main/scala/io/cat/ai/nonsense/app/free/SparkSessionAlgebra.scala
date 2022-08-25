package io.cat.ai.nonsense.app.free

import cats.effect.{Async, ContextShift, ExitCase}
import cats.free.Free
import cats.{Monad, ~>}

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}

trait SparkSessionAlgebra[S] {
  def service[F[_]](v: SparkSessionAlgebra.Service[F]): F[S]
}

object SparkSessionAlgebra { base =>

  type SparkSessionIO[S] = Free[SparkSessionAlgebra, S]

  trait Service[F[_]] extends (SparkSessionAlgebra ~> F) { self =>

    final def apply[A](fa: SparkSessionAlgebra[A]): F[A] =
      fa service self

    def raw[A](f: SparkSession => A): F[A]
    def embed[A](e: Embedded[A]): F[A]
    def delay[A](a: () => A): F[A]
    def handleErrorWith[A](fa: SparkSessionIO[A],
                           f: Throwable => SparkSessionIO[A]): F[A]
    def async[A](k: (Either[Throwable, A] => Unit) => Unit): F[A]
    def asyncF[A](k: (Either[Throwable, A] => Unit) => SparkSessionIO[Unit]): F[A]
    def raiseError[A](e: Throwable): F[A]
    def shift: F[Unit]
    def evalOn[A](ec: ExecutionContext)
                 (fa: SparkSessionIO[A]): F[A]
    def bracketCase[A, B](acquire: SparkSessionIO[A])
                         (use: A => SparkSessionIO[B])
                         (release: (A, ExitCase[Throwable]) => SparkSessionIO[Unit]): F[B]

    def newSession: F[SparkSession]
    def emptyDataFrame: F[DataFrame]
    def close: F[Unit]
  }

  final case class Raw[A](f: SparkSession => A) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service raw f
  }

  final case class Embed[A](embeddedA: Embedded[A]) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service embed embeddedA
  }

  final case class Delay[A](a: () => A) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service delay a
  }

  final case class HandleErrorWith[A](fa: SparkSessionIO[A], f: Throwable => SparkSessionIO[A]) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service handleErrorWith(fa, f)
  }

  final case class AsyncA[A](k: (Either[Throwable, A] => Unit) => Unit) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service async k
  }

  final case class AsyncF[A](k: (Either[Throwable, A] => Unit) => SparkSessionIO[Unit]) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service asyncF k
  }

  final case class BracketCase[A, B](acquire: SparkSessionIO[A],
                                     use: A => SparkSessionIO[B],
                                     release: (A, ExitCase[Throwable]) => SparkSessionIO[Unit]) extends SparkSessionAlgebra[B] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[B] =
      service.bracketCase(acquire)(use)(release)
  }

  final case class RaiseError[A](e: Throwable) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service raiseError[A] e
  }

  final case object Shift extends SparkSessionAlgebra[Unit] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[Unit] =
      service shift
  }

  final case class EvalOn[A](ec: ExecutionContext, fa: SparkSessionIO[A]) extends SparkSessionAlgebra[A] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[A] =
      service.evalOn(ec)(fa)
  }

  final case object NewSession extends SparkSessionAlgebra[SparkSession] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[SparkSession] =
      service newSession
  }

  final case object CreateEmptyDataFrame extends SparkSessionAlgebra[DataFrame] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[DataFrame] =
      service emptyDataFrame
  }

  final case object Close extends SparkSessionAlgebra[Unit] {
    def service[F[_]](service: SparkSessionAlgebra.Service[F]): F[Unit] =
      service close
  }

  val unit: SparkSessionIO[Unit] =
    Free pure[SparkSessionAlgebra, Unit] ()

  def pure[A](a: A): Free[SparkSessionAlgebra, A] =
    Free pure[SparkSessionAlgebra, A] a

  def raw[A](f: SparkSession => A): Free[Raw, A] =
    Free liftF Raw(f)

  def embed[F[_], E, A](e: E, fa: Free[F, A])
                       (implicit ev: Embeddable[F, E]): Free[Embed, A] =
    Free liftF Embed(ev embedded(e, fa))

  def delay[A](a: => A): SparkSessionIO[A] =
    Free liftF[SparkSessionAlgebra, A] Delay(() => a)

  def handleErrorWith[A](fa: SparkSessionIO[A],
                         f: Throwable => SparkSessionIO[A]): SparkSessionIO[A] =
    Free liftF[SparkSessionAlgebra, A] HandleErrorWith(fa, f)

  def async[A](k: (Either[Throwable, A] => Unit) => Unit): SparkSessionIO[A] =
    Free liftF[SparkSessionAlgebra, A] AsyncA(k)

  def asyncF[A](k: (Either[Throwable, A] => Unit) => SparkSessionIO[Unit]): SparkSessionIO[A] =
    Free liftF[SparkSessionAlgebra, A] AsyncF(k)

  def bracketCase[A, B](acquire: SparkSessionIO[A])
                       (use: A => SparkSessionIO[B])
                       (release: (A, ExitCase[Throwable]) => SparkSessionIO[Unit]): SparkSessionIO[B] =
    Free liftF[SparkSessionAlgebra, B] BracketCase(acquire, use, release)

  def raiseError[S](err: Throwable): SparkSessionIO[S] =
    Free liftF[SparkSessionAlgebra, S] RaiseError(err)

  def evalOn[A](ec: ExecutionContext)(fa: SparkSessionIO[A]): Free[SparkSessionAlgebra, A] =
    Free liftF[SparkSessionAlgebra, A] EvalOn(ec, fa)

  val shift: SparkSessionIO[Unit] =
    Free liftF[SparkSessionAlgebra, Unit] Shift

  def newSession: Free[SparkSessionAlgebra, SparkSession] =
    Free liftF[SparkSessionAlgebra, SparkSession] NewSession

  def emptyDataFrame: Free[SparkSessionAlgebra, DataFrame] =
    Free liftF[SparkSessionAlgebra, DataFrame] CreateEmptyDataFrame

  def close: Free[SparkSessionAlgebra, Unit] =
    Free liftF[SparkSessionAlgebra, Unit] Close

  implicit val ContextShiftSparkSessionIO: ContextShift[SparkSessionIO] =
    new ContextShift[SparkSessionIO] {
      def shift: SparkSessionIO[Unit] =
        base shift

      def evalOn[A](ec: ExecutionContext)
                   (fa: SparkSessionIO[A]): Free[SparkSessionAlgebra, A] =
        base.evalOn(ec)(fa)
    }

  trait Implicits {

    implicit val SparkSessionAlgebraEmbeddable: Embeddable[SparkSessionAlgebra, SparkSession] =
      new Embeddable[SparkSessionAlgebra, SparkSession] {
        def embedded[A](j: SparkSession,
                        fa: Free[SparkSessionAlgebra, A]): Embedded.SparkSession[A] =
          Embedded.SparkSession(j, fa)
      }

    implicit val AsyncSessionIO: Async[SparkSessionIO] =
      new Async[SparkSessionIO] {

        val asyncM: Monad[Free[SparkSessionAlgebra, *]] = Free.catsFreeMonadForFree[SparkSessionAlgebra]

        def bracketCase[A, B](acquire: SparkSessionIO[A])
                             (use: A => SparkSessionIO[B])
                             (release: (A, ExitCase[Throwable]) => SparkSessionIO[Unit]): SparkSessionIO[B] =
          base.bracketCase(acquire)(use)(release)

        def pure[A](x: A): SparkSessionIO[A] =
          asyncM pure x

        def handleErrorWith[A](fa: SparkSessionIO[A])
                              (f: Throwable => SparkSessionIO[A]): SparkSessionIO[A] =
          base handleErrorWith(fa, f)

        def raiseError[A](e: Throwable): SparkSessionIO[A] =
          base raiseError[A] e

        def async[A](k: (Either[Throwable,A] => Unit) => Unit): SparkSessionIO[A] =
          base async k

        def asyncF[A](k: (Either[Throwable,A] => Unit) => SparkSessionIO[Unit]): SparkSessionIO[A] =
          base asyncF k

        def flatMap[A, B](fa: SparkSessionIO[A])
                         (f: A => SparkSessionIO[B]): SparkSessionIO[B] =
          asyncM.flatMap(fa)(f)

        def tailRecM[A, B](a: A)
                          (f: A => SparkSessionIO[Either[A, B]]): SparkSessionIO[B] =
          asyncM.tailRecM(a)(f)

        def suspend[A](thunk: => SparkSessionIO[A]): SparkSessionIO[A] =
          asyncM flatten(base delay thunk)
      }
  }
}