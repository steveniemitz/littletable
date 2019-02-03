package com.steveniemitz.littletable

import java.util.function.Supplier

private object Time {
  private final val nowOverride = ThreadLocal.withInitial[Option[Long]](new Supplier[Option[Long]] {
    override def get(): Option[Long] = None
  })

  def now: Long =
    nowOverride.get() match {
      case Some(v) => v
      case None => System.currentTimeMillis()
    }

  def withTimeAt[T](value: Long)(fn: => T): T = {
    val previousValue = nowOverride.get()
    try {
      nowOverride.set(Some(value))
      fn
    } finally {
      nowOverride.set(previousValue)
    }
  }
}
