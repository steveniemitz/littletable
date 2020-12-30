package com.steveniemitz.littletable

import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.{BigtableDataClient, BigtableSession}
import com.google.protobuf.ByteString
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Channel, ManagedChannel, Server}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import org.scalatest.{BeforeAndAfterEach, Inspectors}
import scala.collection.JavaConverters._

private[littletable] trait BigtableTestSuite extends FunTestSuite with Inspectors with BeforeAndAfterEach {
  implicit class ByteStringTransformer(sc: StringContext) {
    def bs(args: Any*): ByteString = ByteString.copyFromUtf8(sc.s(args: _*))
  }

  protected def bs(chars: Char*): ByteString =
    ByteString.copyFrom(chars.map(_.toByte).toArray)

  private var server: Server = _
  private var dataService: BigtableDataService = _
  private var tableAdminService: BigtableAdminService = _

  private var _tableData: ConcurrentHashMap[String, Table] = _
  private var _session: BigtableSession = _

  private var _channel: ManagedChannel = _

  protected def inProcessChannel: Channel = _channel

  protected def tableData: Map[String, Table] = _tableData.asScala.toMap
  protected def session: BigtableSession = _session
  protected def dataClient: BigtableDataClient = _session.getDataClient

  protected final val TestTableName = "projects/derp/instances/derp/tables/test"
  protected def testTable: Table = _tableData.get(TestTableName)

  protected def utf8(value: String): ByteString = ByteString.copyFromUtf8(value)

  override def beforeEach(): Unit = {
    System.setProperty("BIGTABLE_SESSION_SKIP_WARMUP", "true")

    _tableData = new ConcurrentHashMap[String, Table]()
    _tableData.put(TestTableName, new Table(Set("f1")))
    dataService = new BigtableDataService(_tableData)
    tableAdminService = new BigtableAdminService(_tableData)

    val uniqueName = InProcessServerBuilder.generateName

    _channel = InProcessChannelBuilder
      .forName(uniqueName)
      .directExecutor()
      .build()

    server = InProcessServerBuilder
      .forName(uniqueName)
      .directExecutor
      .addService(dataService)
      .addService(tableAdminService)
      .build
      .start

    val bigtableOptions = new BigtableOptions.Builder()
      .setProjectId("derp")
      .setInstanceId("derp")
      .setUserAgent("derp")
      .enableEmulator("localhost", 12345)
      .build()

    _session = new InProcessBigtableSession(bigtableOptions, uniqueName)
  }

  override def afterEach(): Unit = {
    _channel.shutdownNow()
    _channel.awaitTermination(100, TimeUnit.DAYS)
    server.shutdown()
    server.awaitTermination()
  }
}
