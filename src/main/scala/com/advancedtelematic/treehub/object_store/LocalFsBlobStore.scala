package com.advancedtelematic.treehub.object_store

import java.io.File
import java.nio.file.{Files, Path, Paths}

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import com.advancedtelematic.data.DataType.ObjectId
import com.advancedtelematic.treehub.http.Errors
import org.genivi.sota.data.Namespace
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NoStackTrace

trait BlobStore {
  def store(namespace: Namespace, id: ObjectId, blob: Source[ByteString, _]): Future[Done]

  def find(namespace: Namespace, id: ObjectId): Future[Source[ByteString, _]]

  def readFull(namespace: Namespace, id: ObjectId): Future[ByteString]

  def exists(namespace: Namespace, id: ObjectId): Future[Boolean]
}

case class BlobStoreError(msg: String, cause: Throwable = null) extends Throwable(msg, cause) with NoStackTrace


object LocalFsBlobStore {
  private val _log = LoggerFactory.getLogger(this.getClass)

  def apply(root: String)(implicit mat: Materializer, ec: ExecutionContext): LocalFsBlobStore = {
    val f = new File(root)
    if(!f.exists() && !f.getParentFile.canWrite) {
      throw new IllegalArgumentException(s"Could not open $root as local blob store")
    } else if (!f.exists()) {
      Files.createDirectory(f.toPath)
      _log.info(s"Created local fs blob store directory: $root")
    }

    _log.info(s"local fs blob store set to $root")
    new LocalFsBlobStore(f)
  }
}

class LocalFsBlobStore(root: File)(implicit ec: ExecutionContext, mat: Materializer) extends BlobStore {
  override def store(ns: Namespace, id: ObjectId, blob: Source[ByteString, _]): Future[Done] = {
    for {
      path <- Future.fromTry(objectPath(ns, id))
      ioResult <- blob.runWith(FileIO.toPath(path))
      res <- if(ioResult.wasSuccessful) {
        Future.successful(Done)
      } else {
        Future.failed(BlobStoreError(s"Error storing local blob ${ioResult.getError.getLocalizedMessage}", ioResult.getError))
      }
    } yield res
  }

  override def find(ns: Namespace, id: ObjectId): Future[Source[ByteString, _]] = {
    exists(ns, id).flatMap {
      case true => Future.fromTry(objectPath(ns, id).map(FileIO.fromPath(_)))
      case false => Future.failed(Errors.BlobNotFound)
    }
  }

  override def readFull(ns: Namespace, id: ObjectId): Future[ByteString] = {
    find(ns, id).flatMap(_.runWith(Sink.reduce(_ ++ _)))
  }

  override def exists(ns: Namespace, id: ObjectId): Future[Boolean] = {
    val path = objectPath(ns, id).flatMap(p => Try(Files.exists(p)))
    Future.fromTry(path)
  }

  private def objectPath(ns: Namespace, id: ObjectId): Try[Path] = {
    val (prefix, rest) = id.get.splitAt(3)
    val path = Paths.get(root.getAbsolutePath, ns.get, prefix, rest)

    Try {
      if (Files.notExists(path.getParent))
        Files.createDirectories(path.getParent)
      path
    }
  }
}
