name := "matrixMultiply"
organization := "com.orange"
version := "0.1"
scalaVersion := "2.12.10"
onChangedBuildSource := ReloadOnSourceChanges

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion % Provided,
"org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
"com.github.fommil.netlib" % "all" % "1.1.2",
"org.scalactic" %% "scalactic" % "3.0.8",
"org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
"org.specs2" %% "specs2-core" % "4.6.0" % Test,
"org.scalatest"    %% "scalatest"  % "3.0.8" % Test)

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
