package com.steveniemitz.littletable

import com.google.protobuf.ByteString
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.{Function => JFunction}
import scala.jdk.CollectionConverters._

private final class Table(val columnFamilies: Set[String]) {
  private val rows: ConcurrentNavigableMap[ByteString, MutableRow] =
    new ConcurrentSkipListMap[ByteString, MutableRow](ByteStringComparator)

  def getOrCreateRow(key: ByteString): MutableRow =
    rows.computeIfAbsent(
      key,
      new JFunction[ByteString, MutableRow] {
        override def apply(t: ByteString): MutableRow = MutableRow.create(t)
      })

  def deleteRow(key: ByteString): Unit = rows.remove(key)

  def deleteAllRows(): Unit = rows.clear()

  def row(key: ByteString): Option[MutableRow] = Option(rows.get(key))

  def range(
    start: Option[ByteString],
    startIsInclusive: Boolean,
    end: Option[ByteString],
    endIsInclusive: Boolean
  ): Iterable[MutableRow] = {
    val tailMap = start.fold(rows) { s =>
      rows.tailMap(s, startIsInclusive)
    }
    val headMap = end.fold(tailMap) { e =>
      tailMap.headMap(e, endIsInclusive)
    }

    headMap.values().asScala
  }
}
