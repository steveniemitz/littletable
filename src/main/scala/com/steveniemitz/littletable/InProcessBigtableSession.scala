package com.steveniemitz.littletable

import com.google.cloud.bigtable.config.BigtableOptions
import com.google.cloud.bigtable.grpc.BigtableSession
import com.google.cloud.bigtable.grpc.io.ChannelPool
import io.grpc.ManagedChannel
import io.grpc.inprocess.InProcessChannelBuilder

private final class InProcessBigtableSession(opts: BigtableOptions, inProcessChannelName: String)
    extends BigtableSession(opts) {
  override def createChannelPool(
    channelFactory: ChannelPool.ChannelFactory,
    count: Int
  ): ManagedChannel = {
    val factory = new ChannelPool.ChannelFactory {
      override def create(): ManagedChannel = {
        InProcessChannelBuilder
          .forName(inProcessChannelName)
          .directExecutor()
          .build()
      }
    }

    new ChannelPool(factory, count)
  }
}
