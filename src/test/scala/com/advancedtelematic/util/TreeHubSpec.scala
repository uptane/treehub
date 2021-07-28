package com.advancedtelematic.util

import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.treehub.Settings
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

abstract class TreeHubSpec extends AnyFunSuite with Matchers with ScalaFutures with Settings {
  val defaultNs = Namespace("default")
}
