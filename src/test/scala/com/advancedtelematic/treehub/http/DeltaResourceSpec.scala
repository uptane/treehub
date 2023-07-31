package com.advancedtelematic.treehub.http

import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.model.headers.{Location, RawHeader}
import com.advancedtelematic.data.DataType.{CommitTupleOps, DeltaIndexId, StaticDeltaMeta, SuperBlockHash, ValidDeltaId, ValidDeltaIndexId}
import com.advancedtelematic.treehub.db.{ObjectRepositorySupport, StaticDeltaMetaRepositorySupport}
import com.advancedtelematic.util.{ResourceSpec, TreeHubSpec}
import com.advancedtelematic.libats.data.RefinedUtils.RefineTry
import com.advancedtelematic.util.FakeUsageUpdate.CurrentBandwith
import akka.pattern.ask

import scala.concurrent.duration.*
import com.advancedtelematic.common.DigestCalculator
import com.advancedtelematic.libats.messaging_datatype.DataType.Commit
import eu.timepit.refined.api.{RefType, Refined}

import scala.util.Random

class DeltaResourceSpec extends TreeHubSpec with ResourceSpec with ObjectRepositorySupport with StaticDeltaMetaRepositorySupport {

  implicit class SuperblockHashHeaderExt(value: HttpRequest) {
    def withSuperblockHash()(implicit hash: SuperBlockHash): HttpRequest = {
      value ~> RawHeader("x-trx-superblock-hash", hash.value)
    }
  }

  test("GET summary returns 404 Not Found if summary is not in the delta store") {
    Get(apiUri(s"summary")) ~> RawHeader("x-ats-namespace", "notfound") ~> routes ~> check {
      status shouldBe StatusCodes.NotFound
    }
  }

  test("GET on delta superblock returns superblock") {
    val deltaId = "FWnaj5O7BH+K7XnIPyhQFYj9m_0jbRnw7fetZJtfmXE-TLFhE9DIGjBpFS7UbPLBpAPyfTF_nHbmNImcP63xoDY".refineTry[ValidDeltaId].get
    val blob = "superblock data".getBytes()

    implicit val superBlockHash = RefType.applyRef[SuperBlockHash](DigestCalculator.digest()(new Random().nextString(10))).toOption.get

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock")) ~> routes ~> check {
      status shouldBe StatusCodes.OK
      responseAs[Array[Byte]] shouldBe blob
    }
  }

  test("GET on deltas using from/to redirects to url with deltaId") {
    val from = RefType.applyRef[Commit]("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824").toOption.get
    val to = RefType.applyRef[Commit]("82e35a63ceba37e9646434c5dd412ea577147f1e4a41ccde1614253187e3dbf9").toOption.get
    val deltaId = (from, to).toDeltaId

    Get(apiUri(s"deltas?from=${from.value}&to=${to.value}")) ~> routes ~> check {
      status shouldBe StatusCodes.Found
      header[Location].map(_.uri.toString()) should contain(s"/deltas/${deltaId.asPrefixedPath}")
    }
  }

  test("publishes usage to bus") {
    val deltaId = "6qdddbmoaLtYLmZg2Q8OZm7syoxvAOFs0fAXbavojKY-k_F8QpdRP9KlPD6wiS2HNF7WuL1sgfu1tLaJXV6GjIU".refineTry[ValidDeltaId].get
    val blob = "some other data".getBytes

    implicit val superBlockHash = RefType.applyRef[SuperBlockHash](DigestCalculator.digest()(new Random().nextString(10))).toOption.get

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock")) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val usage = fakeUsageUpdate.ask(CurrentBandwith(deltaId.toCommitObjectId))(1.second).mapTo[Long].futureValue
    usage should be >= blob.length.toLong
  }

  test("can store objects") {
    val deltaId = "1c65CmlTrNj0G8VKwvrHcZ7cdi6nhROSD4v1XJHuQs8-TLFhE9DIGjBpFS7UbPLBpAPyfTF_nHbmNImcP63xoDY".refineTry[ValidDeltaId].get
    val to = deltaId.toCommit
    val blob = "some static delta data".getBytes

    implicit val superBlockHash = RefType.applyRef[SuperBlockHash](DigestCalculator.digest()(new Random().nextString(10))).toOption.get

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/0"), blob).withSuperblockHash()(superBlockHash) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/0")).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.RetryWith
    }

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/1"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock"), blob).withSuperblockHash()(superBlockHash) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/1")) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val size1 = staticDeltaMetaRepository.findByTo(defaultNs, to, StaticDeltaMeta.Status.Available).futureValue.headOption.map(_.size)
    size1 should contain(blob.size * 3)
  }

  test("can store superblock") {
    val deltaId = "SyJ3d9TdH8Ycb4hPSGQdArTRIdP9Moywi1Ux_Kzav4o-TLFhE9DIGjBpFS7UbPLBpAPyfTF_nHbmNImcP63xoDY".refineTry[ValidDeltaId].get
    val blob = "some other data".getBytes
    implicit val superBlockHash = RefType.applyRef[SuperBlockHash](DigestCalculator.digest()(new Random().nextString(10))).toOption.get

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock")) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  test("storing superblock makes the static delta available") {
    val deltaId = "JHCp7ENNjvWRcHDmWVi9IeD5NRoqrcdM6swXSC2o7TQ-TLFhE9DIGjBpFS7UbPLBpAPyfTF_nHbmNImcP63xoDY".refineTry[ValidDeltaId].get
    val blob = "some data".getBytes
    implicit val superBlockHash = RefType.applyRef[SuperBlockHash](DigestCalculator.digest()(new Random().nextString(10))).toOption.get

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/0"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/0")) ~> routes ~> check {
      status shouldBe StatusCodes.RetryWith
    }

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"deltas/${deltaId.asPrefixedPath}/0")) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  test("retrieves index in a gvariant containing relevant static deltas and superblocks") {
    val deltaId = "472PluJ+96MzzZM4nTfIzMIqCLaV7XOWikSgajJubqM-419vppKTWOqy0f9_br2d8SOrj3tALC7vig+fytavOPg".refineTry[ValidDeltaId].get
    val blob = "some data".getBytes
    implicit val superBlockHash = RefType.applyRef[SuperBlockHash]("6cb4598306785bdaac4186d4063d04b54a27d4d6ed463520d0a7df2f932887ee").toOption.get

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/0"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    val deltaIndex = RefType.applyRef[DeltaIndexId]("419vppKTWOqy0f9_br2d8SOrj3tALC7vig+fytavOPg.index").toOption.get

    Get(apiUri(s"delta-indexes/${deltaIndex.asPrefixedPath}")) ~> routes ~> check {
      status shouldBe StatusCodes.NotFound
    }

    Post(apiUri(s"deltas/${deltaId.asPrefixedPath}/superblock"), blob).withSuperblockHash() ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }

    Get(apiUri(s"delta-indexes/${deltaIndex.asPrefixedPath}")) ~> routes ~> check {
      status shouldBe StatusCodes.OK
    }
  }
}
