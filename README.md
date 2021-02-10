# LittleTable

[![Build Status](https://travis-ci.org/steveniemitz/littletable.svg?branch=master)](https://travis-ci.org/steveniemitz/littletable)
[![Maven Version](https://img.shields.io/maven-central/v/com.steveniemitz/littletable_2.12?label=littletable_2.12)](http://search.maven.org/#search|gav|1|g:"com.steveniemitz")
[![Mentioned in Awesome Bigtable](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/zrosenbauer/awesome-bigtable)

## Overview

LittleTable is an emulator for [Google Cloud Bigtable](https://cloud.google.com/bigtable/), intended
to replace the emulator distributed with the `gcloud` utility.

It aims to provide full compatibility with the Cloud Bigtable API,
and fill the gaps in the Go-based emulator, such as the `sink` filter.

## Getting the emulator

Maven:
```xml
<dependency>
  <groupId>com.steveniemitz</groupId>
  <artifactId>littletable_2.12</artifactId>
  <version>1.1.0</version>
</dependency>
```

Gradle:
```gradle
compile 'com.steveniemitz:littletable_2.12:1.0.0'
```

sbt:
```sbt
libraryDependencies += "com.steveniemitz" %% "littletable" % "1.0.0"
```

### Other Dependencies

LittleTable assumes you'll "bring your own" dependencies for gRPC as well as [`bigtable-client-core`](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core).
By default, `bigtable-client-core` will also include the required gRPC dependencies, so adding a
dependency to that is all that's required.

See [`build.sbt`](build.sbt) for reasonable defaults.

## Usage
 
`BigtableEmulator.newBuilder` (or `BigtableEmulators.newBuilder` in Java) can be used to obtain an 
emulator builder.  The builder can configure an in-process gRPC transport, or a TCP transport 
(or both).  When using the in-process emulator, the session provided by the built emulator must be 
used.  For advanced usage, `BigtableEmulator.Builder.configureServerBuilder` can be used to 
configure a user-provided gRPC server builder.

```scala
val emulator = 
  BigtableEmulator.newBuilder
    .withInProcess
    .build()

// Start the emulator    
emulator.start()

// Use the client
val session = emulator.session
val rows = session.getDataClient.readFlatRowsList(...)
```

See the [`BigtableTestSuite`](src/test/scala/com/steveniemitz/littletable/BigtableTestSuite.scala) for a full example.
