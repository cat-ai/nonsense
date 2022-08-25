package io.cat.ai.nonsense.app

import java.util.concurrent.Executors.newSingleThreadExecutor

import cats.effect.{ConcurrentEffect, ContextShift, IO}

import io.cat.ai.nonsense.runtime.Hook.onShutdown
import io.cat.ai.nonsense.runtime.{Context, Hook}
import io.cat.ai.nonsense.runtime.module.RuntimeModule

import java.util.concurrent.ExecutorService

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.fromExecutor

trait IORuntimeModule extends Context with RuntimeModule {

  override type F[A] = IO[A]

  private[this] lazy val UnderlyingExecutorService: ExecutorService =
    newSingleThreadExecutor

  implicit lazy val ExecutionContext: ExecutionContext =
    fromExecutor(UnderlyingExecutorService)

  implicit lazy val ContextShift: ContextShift[IO] =
    IO contextShift ExecutionContext

  override implicit lazy val effect: ConcurrentEffect[IO] =
    IO ioConcurrentEffect ContextShift

  lazy val shutdownExecutionContext: Hook[IO] =
    onShutdown(IO(UnderlyingExecutorService shutdown()))
}