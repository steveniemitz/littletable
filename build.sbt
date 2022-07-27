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

crossScalaVersions := Seq("2.11.12", "2.12.10", "2.13.5")

scalaVersion := "2.13.5"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.3"

libraryDependencies += 
  "com.google.api.grpc" % "grpc-google-cloud-bigtable-admin-v2" % "1.8.0" excludeAll
    ExclusionRule(organization = "io.grpc")

libraryDependencies += 
  "com.google.api.grpc" % "grpc-google-cloud-bigtable-v2" % "1.8.0" excludeAll
    ExclusionRule(organization = "io.grpc")

libraryDependencies += "com.google.cloud.bigtable" % "bigtable-client-core" % "1.10.0" % Provided

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.5" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
libraryDependencies += "com.google.cloud.bigtable" % "bigtable-client-core" % "1.10.0" % Test
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % Test
libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % Test

ThisBuild / crossScalaVersions := Seq("2.12.15", "2.13.8")
ThisBuild / githubWorkflowTargetTags ++= Seq("v*")
ThisBuild / githubWorkflowPublishTargetBranches := Seq(RefPredicate.StartsWith(Ref.Tag("v")))
ThisBuild / githubWorkflowPublish := Seq(WorkflowStep.Sbt(List("ci-release")))
ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    List("ci-release"),
    env = Map(
      "PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}",
      "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}",
      "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
      "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
    )
  )
)
