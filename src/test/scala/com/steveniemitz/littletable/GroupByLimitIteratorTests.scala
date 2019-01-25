package com.steveniemitz.littletable

class GroupByLimitIteratorTests extends FunTestSuite {
  test("grouped seq") {
    val data = Seq(1, 1, 1, 2, 2, 2, 3, 4, 5, 5, 6)
    val iter = GroupByLimitIterator[Int](data.iterator, 2)((a, b) => a == b)

    iter.map(_.toSeq).toSeq should be(
      Seq(
        Seq(1, 1),
        Seq(2, 2),
        Seq(3),
        Seq(4),
        Seq(5, 5),
        Seq(6)
      ))
  }
}
