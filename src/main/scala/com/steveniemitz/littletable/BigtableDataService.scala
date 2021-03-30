package com.steveniemitz.littletable

import com.google.bigtable.v2.Mutation.MutationCase
import com.google.bigtable.v2.{Cell => _, Row => _, _}
import com.google.rpc.Code
import com.google.protobuf.{ByteString, BytesValue, StringValue}
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

final class BigtableDataService(tables: ConcurrentHashMap[String, Table])
    extends BigtableGrpc.BigtableImplBase {

  override def mutateRows(
    request: MutateRowsRequest,
    responseObserver: StreamObserver[MutateRowsResponse]
  ): Unit = {
    val table = tables.get(request.getTableName)
    if (table == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      val resp = MutateRowsResponse.newBuilder()
      var idx = 0
      request.getEntriesList.asScala.foreach { e =>
        mutateRow(table, e.getRowKey, e.getMutationsList.asScala)
        resp
          .addEntriesBuilder()
          .setIndex(idx)
          .getStatusBuilder.setCode(Code.OK.getNumber)
        idx += 1
      }
      responseObserver.onNext(resp.build())
      responseObserver.onCompleted()
    }
  }

  override def mutateRow(
    request: MutateRowRequest,
    responseObserver: StreamObserver[MutateRowResponse]
  ): Unit = {
    val table = tables.get(request.getTableName)
    if (table == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      mutateRow(table, request.getRowKey, request.getMutationsList.asScala)
      responseObserver.onNext(MutateRowResponse.getDefaultInstance)
      responseObserver.onCompleted()
    }
  }

  private def mutateRow(table: Table, rowKey: ByteString, mutations: scala.collection.Seq[Mutation]): Unit = {
    val row = table.getOrCreateRow(rowKey)
    row.transact { row =>
      applyMutations(row, mutations)
      if (!row.hasCells) table.deleteRow(row.key)
    }
  }

  private def applyMutations(row: MutableRow, mutations: scala.collection.Seq[Mutation]): Unit = {
    row.transact { row =>
      mutations.foreach { m =>
        m.getMutationCase match {
          case MutationCase.SET_CELL =>
            val sc = m.getSetCell
            val ts =
              if (sc.getTimestampMicros == -1) Time.now * 1000 // millis -> micros
              else sc.getTimestampMicros
            val cell = Cell(sc.getFamilyName, sc.getColumnQualifier, ts, sc.getValue)
            row.setCell(cell)
          case MutationCase.DELETE_FROM_ROW =>
            row.deleteAllCells()
          case MutationCase.DELETE_FROM_FAMILY =>
            row.deleteFamily(m.getDeleteFromFamily.getFamilyName)
          case MutationCase.DELETE_FROM_COLUMN =>
            val dc = m.getDeleteFromColumn
            row.deleteCells(
              dc.getFamilyName,
              dc.getColumnQualifier,
              dc.getTimeRange.getStartTimestampMicros,
              dc.getTimeRange.getEndTimestampMicros)
          case MutationCase.MUTATION_NOT_SET => // noop
        }
      }
    }
  }

  override def readRows(
    request: ReadRowsRequest,
    responseObserver: StreamObserver[ReadRowsResponse]
  ): Unit = {
    val table = tables.get(request.getTableName)
    if (table == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      readRowsImpl(request, responseObserver, table)
    }
  }

  private def readRowsImpl(
    request: ReadRowsRequest,
    responseObserver: StreamObserver[ReadRowsResponse],
    table: Table
  ): Unit = {
    val rowsToFilter = if (!request.hasRows) {
      table.range(None, startIsInclusive = true, None, endIsInclusive = true)
    } else {
      val rowsForKeys = request.getRows.getRowKeysList
        .iterator().asScala
        .flatMap(table.row)

      val rowsForRanges = request.getRows.getRowRangesList
        .iterator().asScala
        .flatMap { rr =>
          val (startRow, startRowInclusive) = rr.getStartKeyCase match {
            case RowRange.StartKeyCase.START_KEY_CLOSED => Some(rr.getStartKeyClosed) -> true
            case RowRange.StartKeyCase.START_KEY_OPEN => Some(rr.getStartKeyOpen) -> false
            case RowRange.StartKeyCase.STARTKEY_NOT_SET => None -> true
          }
          val (endRow, endRowInclusive) = rr.getEndKeyCase match {
            case RowRange.EndKeyCase.END_KEY_CLOSED => Some(rr.getEndKeyClosed) -> true
            case RowRange.EndKeyCase.END_KEY_OPEN => Some(rr.getEndKeyOpen) -> false
            case RowRange.EndKeyCase.ENDKEY_NOT_SET => None -> true
          }

          table.range(startRow, startRowInclusive, endRow, endRowInclusive)
        }

      val deduppedRows = new java.util.TreeMap[ByteString, Row](ByteStringComparator)
      (rowsForKeys ++ rowsForRanges).foreach { r =>
        deduppedRows.put(r.key, r)
      }
      deduppedRows.values().asScala
    }

    val filteredRows = FilterEvaluator.evaluate(rowsToFilter.iterator, request.getFilter)

    val response = processRows(filteredRows)
    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }

  private def processRows(rows: Iterator[Row]): ReadRowsResponse = {
    val response = ReadRowsResponse.newBuilder()
    rows.foreach { row =>
      row.transact { row =>
        val cells = row.cells
        var firstRow = true
        cells.foreach { c =>
          val chunkBuilder = response.addChunksBuilder()
          if (firstRow) {
            chunkBuilder
              .setRowKey(row.key)
              .setFamilyName(StringValue.of(c.columnFamily))
            firstRow = false
          }

          chunkBuilder
            .setFamilyName(StringValue.of(c.columnFamily))
            .setQualifier(BytesValue.of(c.columnQualifier))
            .setTimestampMicros(c.timestamp)
            .setValue(c.value)
        }

        if (response.getChunksCount > 0) {
          response
            .getChunksBuilder(response.getChunksCount - 1)
            .setCommitRow(true)
        }
      }
    }

    response.build()
  }

  override def checkAndMutateRow(
    request: CheckAndMutateRowRequest,
    responseObserver: StreamObserver[CheckAndMutateRowResponse]
  ): Unit = {
    val table = tables.get(request.getTableName)
    if (table == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      checkAndMutateRowImpl(request, responseObserver, table)
    }
  }

  private def checkAndMutateRowImpl(
    request: CheckAndMutateRowRequest,
    responseObserver: StreamObserver[CheckAndMutateRowResponse],
    table: Table
  ): Unit = {
    val tableRow = table.row(request.getRowKey)
    val predicateMatched = tableRow.exists {
      _.transact { tr =>
        val checkPassed =
          FilterEvaluator.evaluate(Iterator.single(tr), request.getPredicateFilter).hasNext
        if (checkPassed) {
          applyMutations(tr, request.getTrueMutationsList.asScala)
        } else {
          applyMutations(tr, request.getFalseMutationsList.asScala)
        }
        checkPassed
      }
    }
    val resp = CheckAndMutateRowResponse
      .newBuilder()
      .setPredicateMatched(predicateMatched)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
