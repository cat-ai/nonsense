import sbt._

object Dependencies {

  private object Versions {
    val Cats            = "2.0.0"
    val FS2             = "2.0.0"
    val Spark           = "2.4.4"
    val ElasticSpark    = "7.10.2"
    val HBase           = "1.4.8"
    val Hadoop          = "2.8.5"
    val Kafka           = "1.0.2"
    val Breeze          = "0.13.2"
    val TypesafeConfig  = "1.4.1"
    val Slf4J           = "1.7.31"
    val Ficus           = "1.5.0"
    val MacwireMacros   = "2.3.7"
  }

  object Compile {

    val Cats =
      List(
        "org.typelevel"                         %% "cats-core"                  % Versions.Cats,
        "org.typelevel"                         %% "cats-kernel"                % Versions.Cats,
        "org.typelevel"                         %% "cats-effect"                % Versions.Cats,
        "org.typelevel"                         %% "cats-free"                  % Versions.Cats
      )

    val FS2 =
      List(
        "co.fs2"                                %% "fs2-core"                   % Versions.FS2,
        "co.fs2"                                %% "fs2-io"                     % Versions.FS2
      )

    val MacwireMacros =
      List(
        "com.softwaremill.macwire"              %% "macros"                     % Versions.MacwireMacros     % Provided
      )

    val Ficus =
      List(
        "com.iheart"                            %% "ficus"                      % Versions.Ficus
      )

    val Spark =
      List(
        "org.apache.spark"                      %% "spark-sql"                  % Versions.Spark,
        "org.apache.spark"                      %% "spark-core"                 % Versions.Spark,
        "org.apache.spark"                      %% "spark-mllib"                % Versions.Spark,
        "org.apache.spark"                      %  "spark-streaming_2.11"       % Versions.Spark,
        "org.apache.spark"                      %% "spark-streaming-kafka-0-10" % Versions.Spark,
        "org.apache.spark"                      %% "spark-hive"                 % Versions.Spark,
        "org.elasticsearch"                     %% "elasticsearch-spark-20"     % Versions.ElasticSpark
      )

    val HBase =
      List(
        "org.apache.hbase"                      % "hbase-client"                % Versions.HBase,
        "org.apache.hbase"                      % "hbase-server"                % Versions.HBase,
        "org.apache.hbase"                      % "hbase-protocol"              % Versions.HBase,
        "org.apache.hbase"                      % "hbase-hadoop-compat"         % Versions.HBase,
        "org.apache.hbase"                      % "hbase-hadoop2-compat"        % Versions.HBase,
        "org.apache.hbase"                      % "hbase-server"                % Versions.HBase             % Provided,
        "org.apache.hbase"                      % "hbase-protocol"              % Versions.HBase             % Provided,
        "org.apache.hbase"                      % "hbase-hadoop-compat"         % Versions.HBase             % Provided,
        "org.apache.hbase"                      % "hbase-hadoop2-compat"        % Versions.HBase             % Provided
      )

    val Hadoop =
      List(
        "org.apache.hadoop"                     % "hadoop-client"               % Versions.Hadoop
      )

    val Breeze =
      List(
        "org.scalanlp"                          % "breeze-natives"              % Versions.Breeze
      )

    val Config =
      List(
        "com.typesafe"                          % "config"                      % Versions.TypesafeConfig
      )

    val Logging =
      List(
        "org.slf4j"                             % "slf4j-api"                   % Versions.Slf4J
      )
  }

  lazy val Cats          = Compile.Cats
  lazy val FS2           = Compile.FS2
  lazy val Ficus         = Compile.Ficus
  lazy val MacwireMacros = Compile.MacwireMacros
  lazy val Common        = Compile.Config
  lazy val Hadoop        = Compile.Hadoop
  lazy val Spark         = Compile.Spark
  lazy val HBase         = Compile.HBase
}