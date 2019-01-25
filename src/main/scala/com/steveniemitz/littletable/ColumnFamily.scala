package com.steveniemitz.littletable

import com.google.protobuf.ByteString
import java.util
import java.util.function.{Function => JFunction}
import scala.collection.JavaConverters._

private final class ColumnFamily(name: String) {
  private val columnQualifiers = new util.TreeMap[ByteString, CellCollection](ByteStringComparator)

  def setCell(cell: Cell): Unit = {
    val cellCollection = columnQualifiers.computeIfAbsent(
      cell.columnQualifier,
      new JFunction[ByteString, CellCollection] {
        override def apply(t: ByteString): CellCollection = new CellCollection
      })
    cellCollection.setCell(cell)
  }

  def deleteCells(columnQualifier: ByteString, startTimestamp: Long, endTimestamp: Long): Unit =
    Option(columnQualifiers.get(columnQualifier))
      .foreach(_.deleteCells(startTimestamp, endTimestamp))

  def hasCells: Boolean = columnQualifiers.values().asScala.exists(_.cells.nonEmpty)

  def cells: Iterator[Cell] = columnQualifiers.values().asScala.iterator.flatMap(_.cells)
}
