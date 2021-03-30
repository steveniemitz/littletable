package com.steveniemitz.littletable

import com.google.common.primitives.UnsignedLongs
import java.util
import java.util.Comparator
import scala.jdk.CollectionConverters._

private object CellCollection {
  private object UnsignedLongComparator extends Comparator[Long] {
    override def compare(o1: Long, o2: Long): Int = UnsignedLongs.compare(o1, o2)
  }
}

private final class CellCollection {
  import CellCollection._

  private val _cells = new util.TreeMap[Long, Cell](UnsignedLongComparator)

  def deleteCells(startTimestamp: Long, endTimestamp: Long): Unit = {
    val subMap =
      if (startTimestamp == 0 && endTimestamp == 0) _cells
      else if (startTimestamp != 0 && endTimestamp == 0) _cells.tailMap(startTimestamp, true)
      else if (startTimestamp == 0 && endTimestamp != 0) _cells.headMap(endTimestamp, false)
      else _cells.subMap(startTimestamp, true, endTimestamp, false)

    subMap.clear()
  }

  def setCell(cell: Cell): Unit = _cells.put(cell.timestamp, cell)

  def cells: Iterator[Cell] = _cells.descendingMap().values().iterator().asScala
}
