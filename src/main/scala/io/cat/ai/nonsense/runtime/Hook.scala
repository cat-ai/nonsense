package io.cat.ai.nonsense.runtime

import cats.effect.Sync

import scala.language.{higherKinds, postfixOps}

trait Hook[F[_]] {
  def startup: F[Unit]
  def shutdown: F[Unit]
}

object Hook {

  private[this] abstract class AbstractHook[F[_]: Sync] extends Hook[F] {
    override def startup: F[Unit]  = Sync[F] unit
    override def shutdown: F[Unit] = Sync[F] unit
  }

  def onShutdown[F[_] : Sync](thunk: => F[Unit]): Hook[F] = new AbstractHook[F] {
    override val shutdown: F[Unit] = Sync[F] suspend thunk
  }

  def onStartup[F[_] : Sync](thunk: => F[Unit]): Hook[F] = new AbstractHook[F] {
    override val startup: F[Unit]  = Sync[F] suspend thunk
  }
}