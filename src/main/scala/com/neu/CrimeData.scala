package com.neu

import cats.effect.IO
import cats.syntax.apply
import com.phasmidsoftware.parse._
import com.phasmidsoftware.table.Table

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

case class CrimeData(
                      lsoa_code: String,
                      borough: String,
                      major_category: String,
                      minor_category: String,
                      value: Int,
                      year: Int,
                      month: Int
                    )

object CrimeData extends TableParserHelper[CrimeData]() {
  def cellParser: CellParser[CrimeData] = cellParser7(apply)
}