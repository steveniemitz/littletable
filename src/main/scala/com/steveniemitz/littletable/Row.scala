package com.steveniemitz.littletable

import com.google.common.primitives.UnsignedLongs
import com.google.protobuf.ByteString
import java.util
import scala.collection.JavaConverters._

private sealed trait Row {
  protected type Self <: Row

  def key: ByteString
  def cells: Iterator[Cell]
  def hasCells: Boolean
  def transact[T](fn: Self => T): T
}

private object ResultRow {
  private final case class EmptyRow(key: ByteString) extends Row {
    protected type Self = EmptyRow

    def cells: Iterator[Cell] = Iterator.empty
    def hasCells: Boolean = false
    def transact[T](fn: Self => T): T = fn(this)
  }

  def empty(key: ByteString): Row = EmptyRow(key)

  def create(key: ByteString, cells: Iterator[Cell]): Row = {
    new ResultRow(key, cells)
  }

  def createFromUnsortedCells(key: ByteString, cells: Iterator[Cell]): Row = {
    new ResultRow(key, cells.toSeq.sorted(CellOrdering).iterator)
  }

  object CellOrdering extends Ordering[Cell] {
    override def compare(x: Cell, y: Cell): Int = {
      var cmp = x.columnFamily.compareTo(y.columnFamily)
      if (cmp != 0) return cmp

      cmp = ByteStringComparator.compare(x.columnQualifier, y.columnQualifier)
      if (cmp != 0) return cmp

      UnsignedLongs.compare(y.timestamp, x.timestamp) // we want the timestamp in reverse order
    }
  }
}

/**
 * An implementation of Row that represents an immutable row read from storage.  Unlike `MutableRow`,
 * this row can have duplicate cells, such as when two filters from an Interleave produce the same
 * cell.
 */
private final class ResultRow private (val key: ByteString, val cells: Iterator[Cell]) extends Row {
  override protected type Self = ResultRow

  override def hasCells: Boolean = cells.nonEmpty

  override def transact[T](fn: ResultRow => T): T = fn(this)
}

private object MutableRow {
  def create(key: ByteString, cells: Iterator[Cell] = Iterator.empty): MutableRow = {
    val r = new MutableRow(key)
    r.transact { r =>
      cells.foreach(c => r.setCell(c))
    }
    r
  }
}

/**
 * An implementation of Row that support applying mutations.
 */
private final class MutableRow private (val key: ByteString) extends Row {
  protected type Self = MutableRow

  private val _lock = new Object()
  private val columnFamilies = new util.TreeMap[String, ColumnFamily]().asScala
  private var txnCount: Int = 0

  private def requireTransaction(): Unit = {
    if (txnCount == 0) throw new IllegalStateException("row must be locked")
  }

  def transact[T](fn: Self => T): T = _lock.synchronized {
    txnCount += 1
    try {
      fn(this)
    } finally {
      txnCount -= 1
    }
  }

  def setCell(cell: Cell): Unit = {
    requireTransaction()

    val cf = columnFamilies.getOrElseUpdate(cell.columnFamily, new ColumnFamily(cell.columnFamily))
    cf.setCell(cell)
  }

  def deleteAllCells(): Unit = {
    requireTransaction()
    columnFamilies.clear()
  }

  def deleteFamily(family: String): Unit = {
    requireTransaction()
    columnFamilies.remove(family)
  }

  def deleteCells(
    family: String,
    qualifier: ByteString,
    startTimestamp: Long,
    endTimestamp: Long
  ): Unit = {
    columnFamilies.get(family).foreach(_.deleteCells(qualifier, startTimestamp, endTimestamp))
  }

  def cells: Iterator[Cell] = {
    requireTransaction()
    // toBuffer is used here to materialize the cell list.
    columnFamilies.valuesIterator.flatMap(_.cells).toBuffer.iterator
  }

  def hasCells: Boolean = {
    requireTransaction()
    columnFamilies.values.exists(_.hasCells)
  }
}
