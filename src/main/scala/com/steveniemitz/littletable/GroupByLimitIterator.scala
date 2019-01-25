package com.steveniemitz.littletable

private object GroupByLimitIterator {
  def apply[T](src: Iterator[T], limit: Int)(cmpFn: (T, T) => Boolean): GroupByLimitIterator[T] =
    new GroupByLimitIterator(src, limit, cmpFn)
}

private final class GroupByLimitIterator[T] private (
  src: Iterator[T],
  limit: Int,
  cmpFn: (T, T) => Boolean)
    extends Iterator[Iterator[T]] {

  private val buf = src.buffered

  private final class ChildIterator() extends Iterator[T] {
    private var curr: T = _
    private var taken: Int = 0

    private def consume(): Unit = {
      while (buf.hasNext && cmpFn(curr, buf.head)) {
        curr = buf.next()
      }
    }

    override def hasNext: Boolean = {
      if (curr != null && buf.hasNext) cmpFn(curr, buf.head)
      else buf.hasNext
    }

    override def next(): T = {
      curr = buf.next()
      val ret = curr

      taken += 1
      if (taken >= limit) consume()

      ret
    }
  }

  override def hasNext: Boolean = buf.hasNext

  override def next(): Iterator[T] = new ChildIterator()
}
