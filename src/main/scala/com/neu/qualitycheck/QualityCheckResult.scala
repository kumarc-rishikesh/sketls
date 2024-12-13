package com.neu.qualitycheck

case class QualityCheckResult(
                               checkName: String,
                               passed: Boolean,
                               failedRecords: Option[Seq[String]] = None
                             )