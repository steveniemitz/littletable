inThisBuild(List(
  organization := "com.steveniemitz",
  homepage := Some(url("https://github.com/steveniemitz/littletable")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "steveniemitz",
      "Steve Niemitz",
      "steveniemitz@gmail.com",
      url("http://steveniemitz.com")
    )
  )
))

name := "littletable"

crossScalaVersions := Seq("2.11.11", "2.12.8")

scalaVersion := "2.12.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += 
  "com.google.api.grpc" % "grpc-google-cloud-bigtable-admin-v2" % "0.44.0" excludeAll 
    ExclusionRule(organization = "io.grpc")

libraryDependencies += 
  "com.google.api.grpc" % "grpc-google-cloud-bigtable-v2" % "0.44.0" excludeAll 
    ExclusionRule(organization = "io.grpc")

libraryDependencies += "com.google.cloud.bigtable" % "bigtable-client-core" % "1.8.0" % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
libraryDependencies += "com.google.cloud.bigtable" % "bigtable-client-core" % "1.8.0" % Test
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % Test
libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % Test
