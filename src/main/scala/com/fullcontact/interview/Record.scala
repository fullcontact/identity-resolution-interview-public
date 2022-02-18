package com.fullcontact.interview

case class Record(ids: Seq[String]) {
  override def toString = {
    ids.mkString(" ")
  }
}

object Record {

  def apply(input: String): Record = {
    Record(input.split("\\s+"))
  }

  // generates a new record with unique ids
  def getMergedRecord(records: Iterable[Record]): Record = {
    Record(records.flatMap(_.ids).toSet.toSeq)
  }

}
