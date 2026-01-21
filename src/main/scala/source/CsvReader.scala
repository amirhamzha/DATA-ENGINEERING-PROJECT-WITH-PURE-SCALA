package source

import scala.io.Source

object CsvReader:

  /** Returns a lazy iterator AND a manual close function */
  def read(path: String, expectedColumns: Int = 1): (Iterator[Array[String]], () => Unit) =
    val source = Source.fromFile(path)
    val lines = source.getLines()

    // skip header
    if lines.hasNext then lines.next()

    // map lines to columns, filter empty or malformed rows
    val rows = lines
      .map(_.trim)
      .filter(line => line.nonEmpty && line.split(",").length >= expectedColumns)
      .map(line => line.split(",").map(_.trim))

    (rows, () => source.close())

end CsvReader
