package com.steveniemitz.littletable

import com.google.bigtable.admin.v2
import com.google.bigtable.admin.v2.{
  BigtableTableAdminGrpc,
  CreateTableRequest,
  DeleteTableRequest,
  DropRowRangeRequest,
  GetTableRequest,
  InstanceName,
  ListTablesRequest,
  ListTablesResponse,
  ColumnFamily => ProtoColumnFamily,
  Table => ProtoTable
}
import com.google.bigtable.v2.TableName
import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class BigtableAdminService(tables: ConcurrentHashMap[String, Table])
    extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {
  override def createTable(
    request: CreateTableRequest,
    responseObserver: StreamObserver[v2.Table]
  ): Unit = {
    val instanceName = InstanceName.parse(request.getParent)
    val tableName =
      TableName.of(instanceName.getProject, instanceName.getInstance(), request.getTableId)
    val table = new Table(request.getTable.getColumnFamiliesMap.asScala.keySet.toSet)
    tables.putIfAbsent(tableName.toString, table)

    responseObserver.onNext(request.getTable)
    responseObserver.onCompleted()
  }

  private def toProtoTable(name: String, table: Table): ProtoTable = {
    val t = ProtoTable
      .newBuilder()
      .setName(name)
      .putAllColumnFamilies(
        table.columnFamilies
          .map(cf => cf -> ProtoColumnFamily.getDefaultInstance)
          .toMap
          .asJava)
    t.build()
  }

  override def listTables(
    request: ListTablesRequest,
    responseObserver: StreamObserver[ListTablesResponse]
  ): Unit = {
    val protoTables = tables.asScala.map {
      case (name, desc) =>
        toProtoTable(name, desc)
    }
    responseObserver.onNext(
      ListTablesResponse
        .newBuilder()
        .addAllTables(protoTables.asJava)
        .build()
    )
    responseObserver.onCompleted()
  }

  override def getTable(
    request: GetTableRequest,
    responseObserver: StreamObserver[ProtoTable]
  ): Unit = {
    val maybeTable = tables.get(request.getName)
    if (maybeTable == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      val protoTable = toProtoTable(request.getName, maybeTable)
      responseObserver.onNext(protoTable)
      responseObserver.onCompleted()
    }
  }

  override def deleteTable(
    request: DeleteTableRequest,
    responseObserver: StreamObserver[Empty]
  ): Unit = {
    val removed = tables.remove(request.getName)
    if (removed == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      responseObserver.onNext(Empty.getDefaultInstance)
      responseObserver.onCompleted()
    }
  }

  override def dropRowRange(
    request: DropRowRangeRequest,
    responseObserver: StreamObserver[Empty]
  ): Unit = {
    val table = tables.get(request.getName)
    if (table == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      if (request.getTargetCase == DropRowRangeRequest.TargetCase.DELETE_ALL_DATA_FROM_TABLE
        && request.getDeleteAllDataFromTable) {
        table.deleteAllRows()
      } else if (request.getTargetCase == DropRowRangeRequest.TargetCase.ROW_KEY_PREFIX) {
        ???
      }
      responseObserver.onNext(Empty.getDefaultInstance)
      responseObserver.onCompleted()
    }
  }
}
