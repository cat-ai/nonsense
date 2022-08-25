package io.cat.ai.nonsense.runtime

case class ExitCode(code: Int = 0)

object ExitCode {
  def withCode(code: Int): ExitCode = new ExitCode(code & 0xFF)
}