package com.steveniemitz.littletable

import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.BigtableSession
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

/**
 * An emulator that provides one or more gRPC servers.
 */
final class BigtableEmulator private (
  inProcessServer: Option[(String, Server)],
  tcpServer: Option[Server]) {
  val servers: Seq[Server] = Seq(inProcessServer.map(_._2), tcpServer).flatten

  private final val serversToStart = mutable.Queue[Server](servers: _*)

  /**
   * A [[BigtableSession]] that is configured to use the emulator service.  If the emulator was
   * built with an in-process transport, it will use that, otherwise it will configure the TCP
   * transport using `BigtableOptions.enableEmulator`
   */
  val session: BigtableSession = {
    // new Builder is deprecated, but we need to use it to allow backwards compatibility with older
    // bigtable client versions.
    val bigtableOptions = new BigtableOptions.Builder()
      .setProjectId("derp")
      .setInstanceId("derp")
      .setUserAgent("derp")
      .enableEmulator("localhost", 12345)

    System.setProperty("BIGTABLE_SESSION_SKIP_WARMUP", "true")
    if (inProcessServer.isDefined) {
      new InProcessBigtableSession(bigtableOptions.build(), inProcessServer.get._1)
    } else if (tcpServer.isDefined) {
      serversToStart.dequeueFirst(_.eq(tcpServer.get))
      tcpServer.get.start()

      bigtableOptions.setPort(tcpServer.get.getPort)
      new BigtableSession(bigtableOptions.build())
    } else {
      throw new IllegalArgumentException
    }
  }

  def start(): Unit = serversToStart.dequeueAll(_ => true).foreach(_.start())

  def shutdown(): Unit = {
    servers.foreach(_.shutdownNow())
    servers.foreach(_.awaitTermination())
  }
}

object BigtableEmulator {
  final class Builder private[BigtableEmulator] () {
    private val tables = new ConcurrentHashMap[String, Table]()
    private var inProcess: Boolean = true
    private var port: Option[Int] = None

    /**
     * Configure this builder to provide a TCP server when built.
     * @param port  The port to listen on
     */
    def withTcp(port: Int): Builder = {
      this.port = Some(port)
      this
    }

    /**
     * Configure this builder to not provide a TCP server when built.
     * @return
     */
    def withoutTcp: Builder = {
      this.port = None
      this
    }

    /**
     * Configure this builder to provide an in-process server when built.
     */
    def withInProcess: Builder = {
      this.inProcess = true
      this
    }

    /**
     * Configure this builder to not provide an in-process server when built.
     */
    def withoutInProcess: Builder = {
      this.inProcess = false
      this
    }

    def build(): BigtableEmulator = {
      val maybeTcpServer = port.map { p =>
        import scala.language.existentials
        val serverBuilder = ServerBuilder.forPort(p)
        configureServerBuilder(serverBuilder)
      }
      val maybeInProcServer = if (inProcess) {
        val uniqueName = InProcessServerBuilder.generateName
        val serverBuilder = InProcessServerBuilder
          .forName(uniqueName)
          .directExecutor

        Some(uniqueName -> configureServerBuilder(serverBuilder))
      } else {
        None
      }
      new BigtableEmulator(maybeInProcServer, maybeTcpServer)
    }

    def configureServerBuilder(builder: ServerBuilder[_]): Server = {
      builder.addService(new BigtableDataService(tables))
      builder.addService(new BigtableAdminService(tables))
      builder.build
    }
  }

  /**
   * Create a new builder instance.  All servers created from a builder will share the same
   * underlying data.  The default configuration is to provide an in-process server and no TCP
   * server (the equivalent of `newBuilder().withInProcess.withoutTcp`).
   */
  def newBuilder(): Builder = new Builder()
}
