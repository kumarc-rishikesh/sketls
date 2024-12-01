package com.neu

import com.phasmidsoftware.parse._

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