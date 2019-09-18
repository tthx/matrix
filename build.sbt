name := "matrixMultiply"
organization := "com.orange"
version := "0.1"
scalaVersion := "2.12.10"
onChangedBuildSource := ReloadOnSourceChanges

import scalariform.formatter.preferences._
includeFilter in scalariformFormat := "*.scala" || "*.sbt"

val sparkVersion = "2.4.4"
val hadoopVersion = "3.2.0"
val netlibVersion = "1.1.2"
val scalacticVersion = "3.0.8"
val scalacheckVersion = "1.14.0"
val specs2coreVersion = "4.6.0"
val scalatestVersion = "3.0.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided
    exclude ("javax.ws.rs", "jsr311-api")
    exclude ("com.sun.jersey", "jersey-server"),
  "com.github.fommil.netlib" % "all" % netlibVersion,
  "org.scalactic" %% "scalactic" % scalacticVersion,
  "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
  "org.specs2" %% "specs2-core" % specs2coreVersion % Test,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test)

scalacOptions in Test ++= Seq("-Yrangepos")

mainClass in assembly := Some("com.orange.tgi.ols.arsec.paas.aacm.matrix.Multiply")

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
  case "about.html"                                 => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA"                      => MergeStrategy.last
  case "META-INF/mailcap"                           => MergeStrategy.last
  case "META-INF/mimetypes.default"                 => MergeStrategy.last
  case "plugin.properties"                          => MergeStrategy.last
  case "log4j.properties"                           => MergeStrategy.last
  case "git.properties"                             => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "matrixMultiply.jar"

/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
fullClasspath in Runtime := (fullClasspath in (Compile, run)).value
