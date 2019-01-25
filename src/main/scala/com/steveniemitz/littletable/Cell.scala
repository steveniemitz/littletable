package com.steveniemitz.littletable

import com.google.protobuf.ByteString
import java.nio.charset.StandardCharsets

private object Cell {
  def apply(
    columnFamily: String,
    columnQualifier: ByteString,
    timestamp: Long,
    value: ByteString
  ): Cell = {
    val c = Cell.apply(columnFamily, columnQualifier, timestamp)
    c.value = value
    c
  }
}

private final case class Cell(
  columnFamily: String,
  columnQualifier: ByteString,
  timestamp: Long,
  labels: Set[String] = Set.empty) {
  var value: ByteString = _

  override def toString: String =
    s"Cell(" +
      s"$columnFamily, " +
      s"${columnQualifier.toString(StandardCharsets.ISO_8859_1)}, " +
      s"$timestamp, " +
      s"${if (value == null) "<null>" else value.toString(StandardCharsets.ISO_8859_1)}, " +
      (if (labels.nonEmpty) labels.mkString("{", ", ", "}") else "") +
      ")"
}
