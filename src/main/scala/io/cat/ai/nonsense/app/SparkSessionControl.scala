package io.cat.ai.nonsense.app

import breeze.util.Lens
import cats.effect.{ExitCase, Resource}
import io.cat.ai.nonsense.app.free.SparkSessionAlgebra
import io.cat.ai.nonsense.app.free.SparkSessionAlgebra.SparkSessionIO
import io.cat.ai.nonsense.app.free.SparkSessionAlgebra.unit

import scala.language.postfixOps

final case class SparkSessionControl(onStart: SparkSessionIO[Unit],
                                     onStop:  SparkSessionIO[Unit],
                                     onError: SparkSessionIO[Unit],
                                     onRun:   SparkSessionIO[Unit]) {
    val resource: Resource[SparkSessionIO, Unit] =
        for {
            _ <- Resource.make(SparkSessionAlgebra.unit)(_ => onRun)
            _ <- Resource.makeCase(onStart) {
                   case (_, exitCase) => exitCase match {
                                           case ExitCase.Completed => onStop
                                           case ExitCase.Error(_) | ExitCase.Canceled => onError
                                         }
                }
        } yield ()
}

object SparkSessionControl {

    val onStart: Lens[SparkSessionControl, SparkSessionIO[Unit]] =
        Lens(_ onStart, (control, session) => control copy(onStart = session))

    val onStop: Lens[SparkSessionControl, SparkSessionIO[Unit]]  =
        Lens(_ onStop, (control, session) => control copy(onStop = session))

    val onError: Lens[SparkSessionControl, SparkSessionIO[Unit]]  =
        Lens(_ onError, (control, session) => control copy(onError = session))

    val onRun: Lens[SparkSessionControl, SparkSessionIO[Unit]]  =
        Lens(_ onRun, (control, session) => control copy(onRun = session))

    val default = SparkSessionControl(unit, unit, unit, unit)
}