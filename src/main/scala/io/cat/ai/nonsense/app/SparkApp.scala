package io.cat.ai.nonsense.app

import cats.data.Kleisli
import cats.effect.{Async, Blocker, Bracket, ContextShift, Resource}
import cats.{Applicative, Defer, Monad, ~>}
import fs2.{Stream => Fs2Stream}
import io.cat.ai.nonsense.app.free.KleisliInterpreterAlgebra.Implicits.SparkSessionImplicits.AsyncSessionIO
import io.cat.ai.nonsense.app.free.SparkSessionAlgebra.SparkSessionIO
import io.cat.ai.nonsense.app.free.{KleisliInterpreterAlgebra, SparkSessionAlgebra}
import io.cat.ai.nonsense.app.util.SparkSessionUtil._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession => ApacheSparkSession}

import scala.concurrent.ExecutionContext
import scala.language.{higherKinds, postfixOps}

abstract class SparkApp {
    def service[F[_]]: SparkApp.Service[F]
}

object SparkApp {

    type Aux[M[_], A0] =
        SparkApp.Service[M] {
            type A = A0
        }

    def apply[M[_], A0](kernel0: A0,
                        sparkSession0: A0 => Resource[M, ApacheSparkSession],
                        interpret0: Interpreter[M],
                        control0: SparkSessionControl): SparkApp.Aux[M, A0] =
        new SparkApp.Service[M] {
          type A            = A0
          val kernel        = kernel0
          val createSession = sparkSession0
          val interpreter   = interpret0
          val control       = control0
        }

    type Interpreter[M[_]] = SparkSessionAlgebra ~> Kleisli[M, ApacheSparkSession, *]

    object fromSparkConfig {
        def apply[M[_]] = new FromSparkConfigUnapplied[M]

        class FromSparkConfigUnapplied[M[_]] {

            def apply[A <: SparkConf](sparkConf: A,
                                      sparkEC:  ExecutionContext,
                                      blocker: Blocker)
                                     (implicit EV: Async[M],
                                               CS: ContextShift[M]): SparkApp.Aux[M, A] = {
                val sparkSession = (sparkConf: A) => {
                    val acquire: M[ApacheSparkSession] = CS.evalOn(sparkEC)(EV delay spark(sparkConf))
                    val release: ApacheSparkSession => M[Unit] = spark => blocker blockOn(EV delay(spark close))

                    Resource.make(acquire)(release)
                }
                val interp = KleisliInterpreterAlgebra[M](blocker).SparkSessionInterpreter
                SparkApp(sparkConf, sparkSession, interp, SparkSessionControl.default)
            }
        }
    }

    sealed abstract class Service[M[_]] { self =>

        type A

        def kernel: A

        def createSession: A => Resource[M, ApacheSparkSession]

        def interpreter: Interpreter[M]

        def configure[B](f: A => M[B]): M[B] =
            f(kernel)

        def control: SparkSessionControl

        def rawExec(implicit EV: Bracket[M, Throwable]): Kleisli[M, ApacheSparkSession, *] ~> M =
            λ[Kleisli[M, ApacheSparkSession, *] ~> M](k => createSession(kernel) use k.run)

        def exec(implicit EV: Bracket[M, Throwable], Defer: Defer[M]): Kleisli[M, ApacheSparkSession, *] ~> M =
            λ[Kleisli[M, ApacheSparkSession, *] ~> M] {
                ka => createSession(kernel) use { sparkSession => control.resource mapK run(sparkSession) use { _ => ka run sparkSession } }
            }

        def rawActionS[T](implicit EV: Monad[M]): Fs2Stream[SparkSessionIO, *] ~> Fs2Stream[M, *] =
            λ[Fs2Stream[SparkSessionIO, *] ~> Fs2Stream[M, *]] { stream =>
              (for {
                spark <- Fs2Stream resource createSession(kernel)
                res   <- stream translate run(spark)
              } yield res) scope
            }

        def actionS(implicit ev: Monad[M]): Fs2Stream[SparkSessionIO, *] ~> Fs2Stream[M, *] =
            λ[Fs2Stream[SparkSessionIO, *] ~> Fs2Stream[M, *]] { stream =>
                (for {
                  session <- Fs2Stream resource createSession(kernel)
                  res     <- (for {
                               _ <- Fs2Stream resource control.resource
                               r <- stream
                             } yield r) translate run(session)
                } yield res) scope
            }

        def rawActionSK[I](implicit ev: Monad[M]): Fs2Stream[Kleisli[SparkSessionIO, I, *], *] ~> Fs2Stream[Kleisli[M, I, *], *] =
            λ[Fs2Stream[Kleisli[SparkSessionIO, I, *], *] ~> Fs2Stream[Kleisli[M, I, *], *]] { stream =>
                (for {
                  session <- Fs2Stream resource createSession(kernel) translate Kleisli.liftK[M, I]
                  res     <- stream translate runKleisli[I](session)
                } yield res) scope
            }
            
        def actionSK[I](implicit ev: Monad[M]): Fs2Stream[Kleisli[SparkSessionIO, I, *], *] ~> Fs2Stream[Kleisli[M, I, *], *] =
            λ[Fs2Stream[Kleisli[SparkSessionIO, I, *], *] ~> Fs2Stream[Kleisli[M, I, *], *]] { stream =>
                (for {
                  session <- Fs2Stream resource createSession(kernel) translate Kleisli.liftK[M, I]
                  res     <- (for {
                               _ <- Fs2Stream resource(control.resource mapK Kleisli.liftK[SparkSessionIO, I])
                               r <- stream
                             } yield r) translate runKleisli[I](session)
                } yield res) scope
            }

        private def run(spark: ApacheSparkSession)
                       (implicit EV: Monad[M]): SparkSessionIO ~> M =
            λ[SparkSessionIO ~> M](_ foldMap interpreter run spark)

        private def runKleisli[B](spark: ApacheSparkSession)
                                 (implicit EV: Monad[M]): Kleisli[SparkSessionIO, B, *] ~> Kleisli[M, B, *] =
            λ[Kleisli[SparkSessionIO, B, *] ~> Kleisli[M, B, *]](f => Kleisli(f run _ foldMap interpreter run spark))

        def mapK[M0[_]](fk: M ~> M0)
                       (implicit Bracket: Bracket[M, Throwable],
                                 Defer: Defer[M0],
                                 Applicative: Applicative[M0]): Aux[M0, A] =
            SparkApp[M0, A](
                kernel,
                createSession andThen(_ mapK fk),
                interpreter andThen λ[Kleisli[M, ApacheSparkSession, *] ~> Kleisli[M0, ApacheSparkSession, *]](_ mapK fk),
                control
            )

        def copy(kernel0: A = self.kernel,
                 session0: A => Resource[M, ApacheSparkSession] = self.createSession,
                 interpret0: Interpreter[M] = self.interpreter,
                 control0: SparkSessionControl = self.control): SparkApp.Aux[M, A] =
            new SparkApp.Service[M] {
                override val kernel: A = kernel0
                override val createSession: A => Resource[M, ApacheSparkSession] = session0
                override val interpreter: Interpreter[M] = interpret0
                override val control: SparkSessionControl = control0
                override type A = self.A
            }
    }
}