package utils

import java.io.{File, PrintWriter}
import scala.util.Using
import java.nio.file.{Files, Paths, Path}
import scala.util.Try

object FileUtils:

  def ensureParent(path: String): Unit =
    val parent = Paths.get(path).getParent
    if parent != null && !Files.exists(parent) then Files.createDirectories(parent)

  /** Write NDJSON (one JSON object per line) from provided JSON strings */
  def writeLines(path: String, lines: Iterator[String]): Unit =
    ensureParent(path)
    Using.resource(new PrintWriter(new File(path))) { writer =>
      lines.foreach { ln => writer.println(ln) }
    }

  /** Append safe helper */
  def appendLine(path: String, line: String): Unit =
    ensureParent(path)
    Using.resource(new java.io.FileWriter(path, true)) { fw =>
      fw.write(line)
      fw.write(System.lineSeparator())
    }

  /** Safe delete (for dev) */
  def deleteIfExists(path: String): Unit =
    Try(Files.deleteIfExists(Paths.get(path)))
    ()

end FileUtils
