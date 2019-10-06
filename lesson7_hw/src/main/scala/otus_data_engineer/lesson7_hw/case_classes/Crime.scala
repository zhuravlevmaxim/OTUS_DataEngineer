package otus_data_engineer.lesson7_hw.case_classes


case class Crime (
                   INCIDENT_NUMBER: Option[String], OFFENSE_CODE: Option[Long], OFFENSE_CODE_GROUP: Option[String],
                   OFFENSE_DESCRIPTION: Option[String], DISTRICT: Option[String], REPORTING_AREA: Option[Int],
                   SHOOTING: Option[String], OCCURRED_ON_DATE: Option[String], YEAR: Option[Int], MONTH: Option[Int],
                   DAY_OF_WEEK: Option[String], HOUR: Option[Int], UCR_PART: Option[String], STREET: Option[String],
                   Lat: Option[BigDecimal], Long: Option[BigDecimal], Location: String
                 )
