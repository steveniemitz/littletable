package com.steveniemitz.littletable

import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.ByteString
import java.util.Comparator

private object ByteStringComparator extends Comparator[ByteString] {
  def isAfterComparer(start: Option[ByteString], startIsInclusive: Boolean): ByteString => Boolean =
    if (start.isEmpty) _ => true
    else {
      val startValue = start.get
      if (startIsInclusive)
        v => compare(v, startValue) >= 0
      else
        v => compare(v, startValue) > 0
    }

  def isBeforeComparer(end: Option[ByteString], endIsInclusive: Boolean): ByteString => Boolean =
    if (end.isEmpty) { _ =>
      true
    } else {
      val endValue = end.get
      if (endIsInclusive)
        v => compare(v, endValue) <= 0
      else
        v => compare(v, endValue) < 0
    }

  private val cmp = UnsignedBytes.lexicographicalComparator()

  override def compare(o1: ByteString, o2: ByteString): Int = {
    val a1 = o1.toByteArray
    val a2 = o2.toByteArray
    cmp.compare(a1, a2)
  }
}
