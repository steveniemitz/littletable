package com.steveniemitz.littletable

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}

abstract class FunTestSuite
  extends FunSuite
    with GeneratorDrivenPropertyChecks
    with Checkers
    with Matchers
