lazy val buildSettings = Seq(
  organization := "io.travisbrown",
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.6", "2.11.7")
)

lazy val compilerOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-unchecked",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

lazy val catsVersion = "0.4.1"
lazy val scalaTestVersion = "3.0.0-M9"
lazy val scalaCheckVersion = "1.12.5"
lazy val disciplineVersion = "0.4"

lazy val baseSettings = Seq(
  scalacOptions ++= compilerOptions ++ (
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 11)) => Seq("-Ywarn-unused-import")
      case _ => Nil
    }
  ),
  scalacOptions in (Compile, console) := compilerOptions,
  scalacOptions in (Compile, test) := compilerOptions,
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.typelevel" %% "cats-core" % catsVersion,
    "org.typelevel" %% "cats-laws" % catsVersion % "test",
    "org.typelevel" %% "discipline" % disciplineVersion
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  ),
  ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := (
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 10)) => false
      case _ => true
    }
  ),
  (scalastyleSources in Compile) <++= unmanagedSourceDirectories in Compile
)

lazy val allSettings = buildSettings ++ baseSettings

lazy val concurrent = project.in(file("."))
  .settings(moduleName := "root")
  .settings(allSettings)
  .aggregate(coreJVM, coreJS)
  .dependsOn(coreJVM, coreJS)

lazy val core = crossProject.crossType(CrossType.Pure)
  .settings(moduleName := "concurrent-core")
  .settings(allSettings: _*)

lazy val coreJVM = core.jvm

lazy val coreJS = core.js

// JVM only
lazy val benchmark = project.dependsOn(coreJVM)
  .settings(
    description := "concurrent benchmark",
    moduleName := "concurrent-benchmark"
  )
  .settings(allSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "util-core" % "6.30.0",
      "org.scalaz" %% "scalaz-concurrent" % "7.2.0"
    )
  )
  .enablePlugins(JmhPlugin)
  .dependsOn(coreJVM)
