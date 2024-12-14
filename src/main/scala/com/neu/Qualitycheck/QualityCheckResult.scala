package com.neu.Qualitycheck

case class QualityCheckResult(
    checkName: String,
    passed: Boolean,
    failedRecords: Option[Seq[String]] = None
)
