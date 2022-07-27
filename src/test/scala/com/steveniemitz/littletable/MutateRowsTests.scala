package com.steveniemitz.littletable

import com.google.bigtable.v2.MutateRowRequest
import com.google.bigtable.v2.ReadRowsRequest
import com.google.cloud.bigtable.grpc.scanner.FlatRow
import com.google.protobuf.ByteString

class MutateRowsTests extends BigtableTestSuite {
  private def readRow(key: ByteString): Option[FlatRow] = {
    val req = ReadRowsRequest
      .newBuilder()
      .setTableName(TestTableName)
    req.getRowsBuilder.addRowKeys(key)

    val rows = dataClient.readFlatRowsList(req.build())
    if (rows.size() == 1) Some(rows.get(0))
    else None
  }

  test("insert new row with no timestamp") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")

    dataClient.mutateRow(req.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1", 0, bs"value1")
        .build()
    )
  }

  test("insert new row with static timestamp") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
      .setTimestampMicros(1000)

    dataClient.mutateRow(req.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1", 1000, bs"value1")
        .build()
    )
  }

  test("insert new row with server-assigned timestamp") {
    val key = bs"rowKey1"

    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
      .setTimestampMicros(-1)

    val now = Time.now
    Time.withTimeAt(now) {
      dataClient.mutateRow(req.build())
    }

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1", now * 1000, bs"value1")
        .build()
    )
  }

  test("insert multiple cells") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq2")
      .setValue(bs"value2")

    dataClient.mutateRow(req.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1", 0, bs"value1")
        .addCell("f1", bs"cq2", 0, bs"value2")
        .build()
    )
  }

  test("delete cells from row") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f2")
      .setColumnQualifier(bs"cq2")
      .setValue(bs"value2")

    dataClient.mutateRow(req.build())

    val del = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    del.addMutationsBuilder().getDeleteFromRowBuilder

    dataClient.mutateRow(del.build())

    readRow(key) shouldBe None
  }

  test("delete cells from family") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f2")
      .setColumnQualifier(bs"cq2")
      .setValue(bs"value2")

    dataClient.mutateRow(req.build())

    val del = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    del
      .addMutationsBuilder()
      .getDeleteFromFamilyBuilder
      .setFamilyName("f1")

    dataClient.mutateRow(del.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f2", bs"cq2", 0, bs"value2")
        .build()
    )
  }

  test("delete cells from column without timestamp filter") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1a")
      .setValue(bs"value1a")
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f2")
      .setColumnQualifier(bs"cq2")
      .setValue(bs"value2")

    dataClient.mutateRow(req.build())

    val del = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    del
      .addMutationsBuilder()
      .getDeleteFromColumnBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")

    dataClient.mutateRow(del.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1a", 0, bs"value1a")
        .addCell("f2", bs"cq2", 0, bs"value2")
        .build()
    )
  }

  test("delete cells from column with timestamp filter") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
      .setTimestampMicros(1000)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
      .setTimestampMicros(1001)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f2")
      .setColumnQualifier(bs"cq2")
      .setValue(bs"value2")
      .setTimestampMicros(1000)

    dataClient.mutateRow(req.build())

    val del = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    del
      .addMutationsBuilder()
      .getDeleteFromColumnBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .getTimeRangeBuilder
      .setStartTimestampMicros(900)
      .setEndTimestampMicros(1001)

    dataClient.mutateRow(del.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1", 1001, bs"value1")
        .addCell("f2", bs"cq2", 1000, bs"value2")
        .build()
    )
  }

  test("update cells") {
    val key = bs"rowKey1"
    val req = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
      .setTimestampMicros(1000)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1")
      .setTimestampMicros(1001)
    req
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f2")
      .setColumnQualifier(bs"cq2")
      .setValue(bs"value2")
      .setTimestampMicros(1000)

    dataClient.mutateRow(req.build())

    val upd = MutateRowRequest
      .newBuilder()
      .setTableName(TestTableName)
      .setRowKey(key)
    upd
      .addMutationsBuilder()
      .getSetCellBuilder
      .setFamilyName("f1")
      .setColumnQualifier(bs"cq1")
      .setValue(bs"value1a")
      .setTimestampMicros(1000)

    dataClient.mutateRow(upd.build())

    val maybeRow = readRow(key)
    maybeRow shouldBe defined

    val row = maybeRow.get
    row should be(
      FlatRow
        .newBuilder()
        .withRowKey(key)
        .addCell("f1", bs"cq1", 1001, bs"value1")
        .addCell("f1", bs"cq1", 1000, bs"value1a")
        .addCell("f2", bs"cq2", 1000, bs"value2")
        .build()
    )
  }
}
