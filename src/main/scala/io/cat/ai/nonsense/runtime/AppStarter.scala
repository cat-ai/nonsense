package io.cat.ai.nonsense.runtime

import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.applicativeError._
import cats.effect.{Concurrent, Effect, Fiber, IO, Sync}


import io.cat.ai.nonsense.runtime.module.RuntimeModule

import scala.language.{higherKinds, postfixOps}
import scala.sys.{ShutdownHookThread, addShutdownHook, exit}
import scala.util.control.NonFatal

trait AppStarter {

  this: Context
    with AppLifecycle
    with RuntimeModule =>

  def run(args : List[String]): F[ExitCode]

  final def main(args : Array[String]): Unit =
    (for {
      fiber    <- toIO(start(run(args toList)))
      exitCode <- toIO(fiber join)
    } yield exitCode) unsafeRunSync match {
                        case ExitCode(0)    => ()
                        case ExitCode(code) => exit(code)
    }

  final def registerShutdownHook(): F[ShutdownHookThread] =
    Sync[F] delay addShutdownHook(toIO(shutdown) unsafeRunSync)

  private[this] def start(actionF: => F[ExitCode]): F[Fiber[F, ExitCode]] = {

    def kick: F[ExitCode] =
      Sync[F] suspend {
        actionF recover {
          case NonFatal(cause) =>
            cause printStackTrace()
            ExitCode withCode 1
        }
      }

    for {
      fiber <- Concurrent[F] start kick
      _     <- Sync[F] delay addShutdownHook(toIO[Unit](fiber cancel) unsafeRunSync)
    } yield fiber
  }

  private[this] def toIO[A](fa : F[A]): IO[A] =
    Effect[F] toIO[A] fa
}