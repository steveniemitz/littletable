package com.steveniemitz.littletable

import com.google.bigtable.admin.v2
import com.google.bigtable.admin.v2.{
  BigtableTableAdminGrpc,
  CheckConsistencyRequest,
  CheckConsistencyResponse,
  CreateTableRequest,
  DeleteTableRequest,
  DropRowRangeRequest,
  GenerateConsistencyTokenRequest,
  GenerateConsistencyTokenResponse,
  GetTableRequest,
  InstanceName,
  ListTablesRequest,
  ListTablesResponse,
  ColumnFamily => ProtoColumnFamily,
  Table => ProtoTable
}
import com.google.bigtable.v2.TableName
import com.google.common.cache.CacheBuilder
import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ThreadLocalRandom, TimeUnit}
import scala.collection.JavaConverters._

private object BigtableAdminService {
  private final class ConsistencyState(val tableName: String, numRequestsToConsistent: Int) {

    assert(numRequestsToConsistent > 0)

    private final val requestsRemaining = new AtomicLong(numRequestsToConsistent)

    def request(): Boolean = requestsRemaining.getAndDecrement() <= 0
  }

  val MaxConsistencyAttempts = 2
}

class BigtableAdminService(tables: ConcurrentHashMap[String, Table])
  extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {

  import BigtableAdminService._

  private final val tokenState = CacheBuilder
    .newBuilder()
    .maximumSize(10000)
    .expireAfterWrite(1, TimeUnit.DAYS)
    .build[String, ConsistencyState]()

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

  override def generateConsistencyToken(
    request: GenerateConsistencyTokenRequest,
    responseObserver: StreamObserver[GenerateConsistencyTokenResponse]
  ): Unit = {
    val table = tables.get(request.getName)
    if (table == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else {
      val token = UUID.randomUUID().toString
      val numRequestsNeeded = ThreadLocalRandom.current().nextInt(1, MaxConsistencyAttempts)
      val state = new ConsistencyState(request.getName, numRequestsNeeded)
      tokenState.put(token, state)
      val response = GenerateConsistencyTokenResponse
        .newBuilder()
        .setConsistencyToken(token)
        .build()

      responseObserver.onNext(response)
      responseObserver.onCompleted()
    }
  }

  override def checkConsistency(
    request: CheckConsistencyRequest,
    responseObserver: StreamObserver[CheckConsistencyResponse]
  ): Unit = {
    val state = tokenState.getIfPresent(request.getConsistencyToken)
    if (state == null) {
      responseObserver.onError(Status.NOT_FOUND.asException())
    } else if (state.tableName != request.getName) {
      responseObserver.onError(Status.INVALID_ARGUMENT.asException())
    } else {
      val isConsistent = state.request()
      if (isConsistent) {
        tokenState.invalidate(request.getConsistencyToken)
      }

      val response = CheckConsistencyResponse
        .newBuilder()
        .setConsistent(isConsistent)
        .build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    }
  }
}
