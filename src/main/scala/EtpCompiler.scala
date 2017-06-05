import java.io.File

import org.apache.avro.Schema
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.generic.GenericData.StringType

import scala.collection.JavaConverters._
/**
  * Created by Chris on 6/5/17.
  */
object EtpCompiler {
  def main(args: Array[String]): Unit = {
    val inputFiles = getRecursiveListOfFiles(new File(args(0)))

    val onlyFiles = inputFiles.filter(!_.isDirectory)
    val sortedFiles = avrohugger.filesorter.AvscFileSorter.sortSchemaFiles(onlyFiles)

    val parser = new Schema.Parser
    sortedFiles.foreach((i: File) => {
      val schema = parser.parse(i)
      val compiler = new SpecificCompiler(schema)
      compiler.setStringType(StringType.String)
      compiler.setEnableDecimalLogicalType(true)
      compiler.compileToDestination(i, new File(args(1)))
    })
    println("done.")
  }

  def getRecursiveListOfFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }
}
