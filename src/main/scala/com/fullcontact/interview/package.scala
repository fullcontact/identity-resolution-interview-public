package com.fullcontact

package object interview {
  case class RecordRow (
    rowId: Long,
    identifiers: String // space separated list of identifiers
  )

  case class QueryRow (
    identifier: String
  )

  case class RecordRowExploded (
    rowId: Long,
    identifier: String // single identifier
  )

  case class IdentifierWithRow (
    identifier: String,
    rowWithIdentifier: Option[String]
  )

  case class IdentifierWithRelatedIds (
    identifier: String,
    relatedIdentifiers: Option[String]
  )
}
