# LittleTable

## Overview

LittleTable is an emulator for Google's Bigtable intended to 
replace the emulator distributed with the `gcloud` utility.

It aims to provide full compatibility with the Cloud Bigtable API,
and fill the gaps left by the go based emulator, including support 
for matching binary data with RE2 regexes and the `sink` filter.

## Getting the libraries

Maven:
```
<dependency>
  <groupId>com.steveniemitz</groupId>
  <artifactId>littletable_2.12</artifactId>
  <version>1.0.0</version>
</dependency>
```

Gradle:
```
compile 'com.steveniemitz:littletable_2.12:1.0.0'
```

sbt:
```
libraryDependencies += "com.steveniemitz" %% "littletable" % "1.0.0"
```

## Usage
 
`BigtableEmulator.newBuilder` (or `BigtableEmulators.newBuilder` in Java) can be used to obtain an 
emulator builder.  The builder can configure an in-process gRPC transport, or a TCP transport 
(or both).  When using the in-process emulator, the session provided by the built emulator must be 
used.  For advanced usage, `BigtableEmulator.Builder.configureServerBuilder` can be used to 
configure a user-provided gRPC server builder.

```$scala
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

See the `BigtableTestSuite` for a full example.
