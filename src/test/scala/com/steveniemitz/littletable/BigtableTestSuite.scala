package com.steveniemitz.littletable

import com.google.bigtable.admin.v2.CreateTableRequest
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableSession}
import com.google.protobuf.ByteString
import org.scalatest.{BeforeAndAfterEach, Inspectors}

private[littletable] trait BigtableTestSuite extends FunTestSuite with Inspectors with BeforeAndAfterEach {
  implicit class ByteStringTransformer(sc: StringContext) {
    def bs(args: Any*): ByteString = ByteString.copyFromUtf8(sc.s(args: _*))
  }

  private var _emu: BigtableEmulator = _
  private var _session: BigtableSession = _

  protected def session: BigtableSession = _session
  protected def dataClient: BigtableDataClient = _session.getDataClient

  protected final val TestTableName = "projects/derp/instances/derp/tables/test"

  override def beforeEach(): Unit = {
    val emu = BigtableEmulator.newBuilder()
      .withInProcess
      .build()

    _session = emu.session
    _emu = emu
    _emu.start()
    _session.getTableAdminClient.createTable(
      CreateTableRequest.newBuilder()
        .setTableId("test")
        .setParent("projects/derp/instances/derp")
        .build()
    )
  }

  override def afterEach(): Unit = {
    _emu.shutdown()
    _session.close()
  }
}
