package com.advancedtelematic.treehub.db

import com.advancedtelematic.data.DataType.{DeltaId, StaticDeltaMeta, SuperBlockHash}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.Commit
import com.advancedtelematic.libats.slick.codecs.SlickRefined.*
import com.advancedtelematic.libats.slick.db.SlickAnyVal.*
import com.advancedtelematic.libats.slick.db.SlickExtensions.*
import com.advancedtelematic.treehub.http.Errors
import slick.jdbc.MySQLProfile.api.*

import scala.concurrent.{ExecutionContext, Future}

trait StaticDeltaMetaRepositorySupport {
  def staticDeltaMetaRepository(implicit db: Database, ec: ExecutionContext) = new StaticDeltaMetaRepository
}

protected class StaticDeltaMetaRepository()(implicit db: Database, ec: ExecutionContext) {

  import Schema.{staticDeltaStatusMapper, staticDeltas}

  def incrementSize(ns: Namespace, id: DeltaId, inc: Long): Future[Unit] = {
    val io = sql"""
    UPDATE #${staticDeltas.baseTableRow.tableName} sd SET size = sd.size + $inc
    WHERE namespace = ${ns.get} and id = ${id.value}
    LIMIT 1
    """.asUpdate.flatMap {
      case size if size < 1 => DBIO.failed(Errors.StaticDeltaDoesNotExist)
      case _ => DBIO.successful(())
    }

    db.run(io)
  }

  def find(ns: Namespace, deltaId: DeltaId): Future[StaticDeltaMeta] = db.run {
    staticDeltas
      .filter(_.namespace === ns)
      .filter(_.id === deltaId)
      .result
      .headOption
      .failIfNone(Errors.StaticDeltaDoesNotExist)
  }

  def findByTo(ns: Namespace, to: Commit, status: StaticDeltaMeta.Status): Future[Seq[StaticDeltaMeta]] = db.run {
    staticDeltas
      .filter(_.namespace === ns)
      .filter(_.to === to)
      .filter(_.status === status)
      .result
  }

  def setStatus(ns: Namespace, id: DeltaId, status: StaticDeltaMeta.Status): Future[Unit] = db.run {
    staticDeltas
      .filter(_.namespace === ns)
      .filter(_.id === id)
      .map(_.status)
      .update(status)
      .map(_ => ())
  }

  def persistIfValid(ns: Namespace, id: DeltaId, to: Commit, superblockHash: SuperBlockHash): Future[StaticDeltaMeta] = {
    val io = staticDeltas
      .filter(_.namespace === ns)
      .filter(_.id === id)
      .take(1)
      .result
      .headOption
      .flatMap {
        case Some(r) if r.superblockHash == superblockHash =>
          DBIO.successful(r)
        case Some(_) =>
          DBIO.failed(Errors.StaticDeltaExists(id, superblockHash))
        case None =>
          val sdm = StaticDeltaMeta(ns, id, to, superblockHash, 0, StaticDeltaMeta.Status.Uploading)
          (staticDeltas += sdm).map(_ => sdm)
      }

    db.run(io)
  }
}