package io.cat.ai.nonsense.runtime

import io.cat.ai.nonsense.runtime.module.RuntimeModule

import scala.language.{higherKinds, postfixOps}

import cats.implicits._

trait AppLifecycle {

  this: Context with RuntimeModule =>

  def wholeLifecycle: Set[Hook[F]]

  def startup(): F[Unit] =
    wholeLifecycle.toList foldMapM(_ startup)

  def shutdown(): F[Unit] =
    wholeLifecycle.toList foldMapM(_ shutdown)
}