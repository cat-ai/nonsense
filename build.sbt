import Dependencies._

name := "nonsense"

version := "0.1"

scalaVersion := "2.11.12"

ThisBuild / organization := "io.cat.ai.nonsense"

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)

val rootProjectLibraryDependencies = Cats ::: FS2 ::: Ficus ::: Common ::: Hadoop ::: Spark

lazy val root = (project in file("."))
  .settings(
    name := "spark-hive-hbase-test",
    scalacOptions += "-Ypartial-unification",
    conflictManager := ConflictManager.latestRevision,
    unmanagedBase := baseDirectory.value / "lib",
    libraryDependencies ++= rootProjectLibraryDependencies
  )