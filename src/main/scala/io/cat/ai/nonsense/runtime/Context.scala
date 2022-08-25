package io.cat.ai.nonsense.runtime

import scala.language.higherKinds

trait Context {
  type F[_]
}