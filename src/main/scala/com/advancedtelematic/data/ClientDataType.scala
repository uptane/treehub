package com.advancedtelematic.data

import com.advancedtelematic.libats.messaging_datatype.DataType.Commit

object ClientDataType {
  case class StaticDelta(from: Commit, to: Commit, size: Long)

  object StaticDelta {

    import io.circe.{Decoder, Encoder}
    import com.advancedtelematic.libats.codecs.CirceRefined.*
    import io.circe.generic.semiauto.{deriveEncoder, deriveDecoder}

    implicit val Encoder: Encoder[StaticDelta] = deriveEncoder[StaticDelta]
    implicit val Decoder: Decoder[StaticDelta] = deriveDecoder[StaticDelta]
  }
}
