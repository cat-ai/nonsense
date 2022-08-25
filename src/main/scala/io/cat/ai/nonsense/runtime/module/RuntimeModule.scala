package io.cat.ai.nonsense.runtime.module

import cats.effect.ConcurrentEffect
import io.cat.ai.nonsense.runtime.Context

trait RuntimeModule {

  this: Context =>

  implicit def effect: ConcurrentEffect[F]
}