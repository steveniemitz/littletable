package com.steveniemitz.littletable

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc
import com.google.bigtable.admin.v2.CheckConsistencyRequest
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest

class TableAdminTests extends BigtableTestSuite {
  test("creates consistency token") {
    val stub = BigtableTableAdminGrpc.newBlockingStub(inProcessChannel)
    val tokenResponse = stub.generateConsistencyToken(
      GenerateConsistencyTokenRequest.newBuilder().setName(TestTableName).build())

    tokenResponse.getConsistencyToken should not be null
  }

  test("checks consistency token") {
    val stub = BigtableTableAdminGrpc.newBlockingStub(inProcessChannel)
    val tokenResponse = stub.generateConsistencyToken(
      GenerateConsistencyTokenRequest.newBuilder().setName(TestTableName).build())

    val checkRequest = CheckConsistencyRequest
      .newBuilder()
      .setConsistencyToken(tokenResponse.getConsistencyToken)
      .setName(TestTableName)
      .build()

    var attempts = 0
    while (!stub.checkConsistency(checkRequest).getConsistent) {
      attempts += 1
      attempts should be < BigtableAdminService.MaxConsistencyAttempts
    }
    attempts should be >= 1
  }

}
