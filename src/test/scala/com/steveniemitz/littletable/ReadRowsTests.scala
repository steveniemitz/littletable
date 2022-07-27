package com.steveniemitz.littletable

import com.google.bigtable.v2.MutateRowsRequest
import com.google.bigtable.v2.Mutation
import com.google.bigtable.v2.ReadRowsRequest
import com.google.cloud.bigtable.grpc.scanner.FlatRow
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

class ReadRowsTests extends BigtableTestSuite {
  test("readRows - key lookup") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getRowsBuilder.addRowKeys(bs"testRow1")

    val rows = dataClient.readFlatRowsList(req.build())
    rows should have length 1

    val expected = FlatRow
      .newBuilder()
      .withRowKey(bs"testRow1")
      .addCell("f1", bs"cq1", 1000, bs"value1")
      .addCell("f1", bs"cq2", 1000, bs"value2")
      .addCell("f2", bs"cq3", 1000, bs"value3")
      .build()

    rows.get(0) should be(expected)
  }

  private def cell(
    family: String,
    qualifier: ByteString,
    timestamp: Long,
    value: ByteString
  ): Mutation = {
    val mut = Mutation.newBuilder()
    mut.getSetCellBuilder
      .setFamilyName(family)
      .setColumnQualifier(qualifier)
      .setTimestampMicros(timestamp)
      .setValue(value)
    mut.build()
  }

  private def insertRows(): Unit = {
    val req = MutateRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req
      .addEntriesBuilder()
      .setRowKey(bs"testRow1")
      .addMutations(cell("f1", bs"cq1", 1000, bs"value1"))
      .addMutations(cell("f1", bs"cq2", 1000, bs"value2"))
      .addMutations(cell("f2", bs"cq3", 1000, bs"value3"))

    req
      .addEntriesBuilder()
      .setRowKey(bs"testRow2")
      .addMutations(cell("f1", bs"cq1", 2000, bs"value10"))
      .addMutations(cell("f1", bs"cq2", 2000, bs"value20"))
      .addMutations(cell("f2", bs"cq3", 2000, bs"value30"))

    req
      .addEntriesBuilder()
      .setRowKey(bs"testRow4")
      .addMutations(cell("f1", bs"cq1", 3000, bs"value100"))
      .addMutations(cell("f1", bs"cq2", 3000, bs"value200"))
      .addMutations(cell("f2", bs"cq3", 3000, bs"value300"))

    dataClient.mutateRows(req.build())
  }

  test("readRows - scan") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getRowsBuilder
      .addRowRangesBuilder()
      .setStartKeyClosed(bs"testRow0")
      .setEndKeyOpen(bs"testRow3")

    val rows = dataClient.readFlatRowsList(req.build())
    val expected1 = FlatRow
      .newBuilder()
      .withRowKey(bs"testRow1")
      .addCell("f1", bs"cq1", 1000, bs"value1")
      .addCell("f1", bs"cq2", 1000, bs"value2")
      .addCell("f2", bs"cq3", 1000, bs"value3")
      .build()
    val expected2 = FlatRow
      .newBuilder()
      .withRowKey(bs"testRow2")
      .addCell("f1", bs"cq1", 2000, bs"value10")
      .addCell("f1", bs"cq2", 2000, bs"value20")
      .addCell("f2", bs"cq3", 2000, bs"value30")
      .build()

    rows should contain theSameElementsAs Seq(expected1, expected2)
  }

  test("readRows - row key regex") {
    insertRows()

    val binaryRowKey = bs('t', 'e', 's', 't', 0xff, 0xff, 'w', '1')
    val insertReq = MutateRowsRequest
      .newBuilder()
      .setTableName(TestTableName)

    insertReq
      .addEntriesBuilder()
      .setRowKey(binaryRowKey)
      .addMutations(cell("f1", bs"cq1", 4000, bs"value100"))
      .addMutations(cell("f1", bs"cq2", 4000, bs"value200"))
      .addMutations(cell("f2", bs"cq3", 4000, bs"value300"))

    dataClient.mutateRows(insertReq.build())

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder.setRowKeyRegexFilter(bs"test\\C{2}w[1-2]")

    val rows = dataClient.readFlatRowsList(req.build())
    val expected1 = FlatRow
      .newBuilder()
      .withRowKey(bs"testRow1")
      .addCell("f1", bs"cq1", 1000, bs"value1")
      .addCell("f1", bs"cq2", 1000, bs"value2")
      .addCell("f2", bs"cq3", 1000, bs"value3")
      .build()
    val expected2 = FlatRow
      .newBuilder()
      .withRowKey(bs"testRow2")
      .addCell("f1", bs"cq1", 2000, bs"value10")
      .addCell("f1", bs"cq2", 2000, bs"value20")
      .addCell("f2", bs"cq3", 2000, bs"value30")
      .build()
    val expected3 = FlatRow
      .newBuilder()
      .withRowKey(binaryRowKey)
      .addCell("f1", bs"cq1", 4000, bs"value100")
      .addCell("f1", bs"cq2", 4000, bs"value200")
      .addCell("f2", bs"cq3", 4000, bs"value300")
      .build()
    rows should contain theSameElementsAs Seq(expected1, expected2, expected3)
  }

  test("readRows - column range filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder.getColumnRangeFilterBuilder
      .setFamilyName("f1")
      .setStartQualifierClosed(bs"cq1")
      .setEndQualifierOpen(bs"cq2")

    val rows = dataClient.readFlatRowsList(req.build())
    val expected = Seq(
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow1")
        .addCell("f1", bs"cq1", 1000, bs"value1")
        .build(),
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow2")
        .addCell("f1", bs"cq1", 2000, bs"value10")
        .build(),
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow4")
        .addCell("f1", bs"cq1", 3000, bs"value100")
        .build()
    )

    rows should contain theSameElementsAs expected
  }

  test("readRows - strip value filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder.setStripValueTransformer(true)

    val rows = dataClient.readFlatRowsList(req.build())
    rows should have size 3

    forAll(rows) { r =>
      r.getCells should have size 3
      forAll(r.getCells) { c =>
        c.getValue.size() should be(0)
      }
    }
  }

  test("readRows - block all filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder.setBlockAllFilter(true)

    val rows = dataClient.readFlatRowsList(req.build())
    rows should have size 0
  }

  test("readRows - pass all filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder.setPassAllFilter(true)

    val rows = dataClient.readFlatRowsList(req.build())
    rows should have size 3
  }

  test("readRows - timestamp range filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder.getTimestampRangeFilterBuilder
      .setStartTimestampMicros(1000)
      .setEndTimestampMicros(2001)

    val rows = dataClient.readFlatRowsList(req.build())
    rows should have size 2
  }

  test("readRows - chain") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    val chain = req.getFilterBuilder.getChainBuilder

    chain
      .addFiltersBuilder()
      .getColumnRangeFilterBuilder
      .setFamilyName("f1")
      .setStartQualifierClosed(bs"cq1")
      .setEndQualifierOpen(bs"cq2")

    chain
      .addFiltersBuilder()
      .getTimestampRangeFilterBuilder
      .setStartTimestampMicros(1000)
      .setEndTimestampMicros(2001)

    val rows = dataClient.readFlatRowsList(req.build())
    val expected = Seq(
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow1")
        .addCell("f1", bs"cq1", 1000, bs"value1")
        .build(),
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow2")
        .addCell("f1", bs"cq1", 2000, bs"value10")
        .build()
    )

    rows should contain theSameElementsAs expected
  }

  test("readRows - interleave") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    val interleave = req.getFilterBuilder.getInterleaveBuilder

    interleave
      .addFiltersBuilder()
      .getColumnRangeFilterBuilder
      .setFamilyName("f1")
      .setStartQualifierClosed(bs"cq1")
      .setEndQualifierOpen(bs"cq2")

    interleave
      .addFiltersBuilder()
      .getColumnRangeFilterBuilder
      .setFamilyName("f2")
      .setStartQualifierClosed(bs"cq3")
      .setEndQualifierOpen(bs"cq4")

    val rows = dataClient.readFlatRowsList(req.build())
    val expected = Seq(
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow1")
        .addCell("f1", bs"cq1", 1000, bs"value1")
        .addCell("f2", bs"cq3", 1000, bs"value3")
        .build(),
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow2")
        .addCell("f1", bs"cq1", 2000, bs"value10")
        .addCell("f2", bs"cq3", 2000, bs"value30")
        .build(),
      FlatRow
        .newBuilder()
        .withRowKey(bs"testRow4")
        .addCell("f1", bs"cq1", 3000, bs"value100")
        .addCell("f2", bs"cq3", 3000, bs"value300")
        .build()
    )

    rows should contain theSameElementsAs expected
  }

  test("readRows - family regex filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getFilterBuilder
      .setFamilyNameRegexFilter("\\C[0,1]")

    val rows = dataClient.readFlatRowsList(req.build())
    forAll(rows) { r =>
      forAll(r.getCells) { c =>
        c.getFamily should be("f1")
      }
    }
  }

  test("readRows - sink") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)

    val chain = req.getFilterBuilder.getChainBuilder
    chain.addFiltersBuilder().setSink(true)
    chain.addFiltersBuilder().setBlockAllFilter(true)

    val rows = dataClient.readFlatRowsList(req.build())
    rows should have size 3
  }

  test("readRows - invalid interleave") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)

    val interleave = req.getFilterBuilder.getInterleaveBuilder
    interleave.addFiltersBuilder().setCellsPerColumnLimitFilter(1)

    the[StatusRuntimeException] thrownBy {
      dataClient.readFlatRowsList(req.build())
    } should have message "INVALID_ARGUMENT: Interleave must contain at least two RowFilters"
  }

  test("readRows - invalid column range filter") {
    insertRows()

    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)

    val columnRange = req.getFilterBuilder.getColumnRangeFilterBuilder
    columnRange
      .setStartQualifierClosed(ByteString.copyFromUtf8("a"))
      .setEndQualifierOpen(ByteString.copyFromUtf8("b"))

    the[StatusRuntimeException] thrownBy {
      dataClient.readFlatRowsList(req.build())
    } should have message "INVALID_ARGUMENT: Error in field 'column_range_filter' : Invalid id for collection columnFamilies : Length should be between [1,64], but found 0"
  }
}
