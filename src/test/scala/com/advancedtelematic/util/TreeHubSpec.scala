package com.advancedtelematic.util

import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.treehub.Settings
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite

abstract class TreeHubSpec extends AnyFunSuite with org.scalatest.matchers.should.Matchers with ScalaFutures with Settings {
  val defaultNs = Namespace("default")
  val testDbConfig: Config = ConfigFactory.load().getConfig("ats.treehub.database")
}

