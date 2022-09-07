package com.quantexa.assessments.scoringModel

import com.quantexa.assessments.accounts.AccountAssessment.{AccountData, CustomerAccountOutput}
import com.quantexa.assessments.customerAddresses.CustomerAddress.{AddressData, AddressRawData, addressDS, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.lit


/***
  * Part of the Quantexa solution is to flag high risk countries as a link to these countries may be an indication of
  * tax evasion.
  *
  * For this question you are required to populate the flag in the ScoringModel case class where the customer has an
  * address in the British Virgin Islands.
  *
  * This flag must be then used to return the number of customers in the dataset that have a link to a British Virgin
  * Islands address.
  */

object ScoringModel extends App {


  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  import spark.implicits._
  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[AccountData],
                               //Addresses for this customer
                               address: Seq[AddressData]
                             )

  case class ScoringModel(
                           customerId: String,
                           forename: String,
                           surname: String,
                           //Accounts for this customer
                           accounts: Seq[AccountData],
                           //Addresses for this customer
                           address: Seq[AddressData],
                           linkToBVI: Boolean
                         )

  val CustomerDocumentDS = spark.read.parquet("src/main/resources/CustomerDocumentDS.parquet").as[CustomerDocument]
  CustomerDocumentDS.show()

  def populateFlag(c: CustomerDocument): Boolean = {
    c.address.exists(x => x.toString().toUpperCase().contains("BRITISH VIRGIN ISLANDS") )
    //ScoringModel(c.customerId, c.forename, c.surname, c.accounts, c.address, linkToBVI)
  }

    val scoringModel: Dataset[ScoringModel] =
    CustomerDocumentDS.map(c => (c.customerId, c.forename, c.surname, c.accounts, c.address, populateFlag(c)))
    .toDF("customerId","forename","surname","accounts","address","linkToBVI").as[ScoringModel]
  scoringModel.show()

  println(s"The number of customers  that have a link to a British : ${scoringModel.filter("linkToBVI == true").count()}")
  scoringModel.write.mode("overwrite").parquet("src/main/resources/scoringModel.parquet")

}