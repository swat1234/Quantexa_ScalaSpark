package com.quantexa.assessments.accounts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


/***
  * A common problem we face at Quantexa is having lots of disjointed raw sources of data and having to aggregate and collect
  * relevant pieces of information into hierarchical case classes which we refer to as Documents. This exercise simplifies
  * the realities of this with just two sources of high quality data however reflects the types of transformations we must
  * perform.
  *
  * You have been given customerData and accountData. This has been read into a DataFrame for you and then converted into a
  * Dataset of the given case classes.
  *
  * If you run this App you will see the top 20 lines of the Datasets provided printed to console
  *
  * This allows you to use the Dataset API which includes .map/.groupByKey/.joinWith ect.
  * But a Dataset also includes all of the DataFrame functionality allowing you to use .join ("left-outer","inner" ect)
  *
  * https://spark.apache.org/docs/latest/sql-programming-guide.html
  *
  * The challenge here is to group, aggregate, join and map the customer and account data given into the hierarchical case class
  * customerAccountoutput. We would prefer you to write using the Scala functions instead of spark.sql() if you choose to perform
  * Dataframe transformations. We also prefer the use of the Datasets API.
  *
  * Example Answer Format:
  *
  * val customerAccountOutputDS: Dataset[customerAccountOutput] = ???
  * customerAccountOutputDS.show(1000,truncate = false)
  *
  * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
  * |customerId|forename   |surname   |accounts                                                             |numberAccounts|totalBalance|averageBalance   |
  * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
  * |IND0001   |Christopher|Black     |[]                                                                   |0             |0           |0.0              |
  * |IND0002   |Madeleine  |Kerr      |[[IND0002,ACC0155,323], [IND0002,ACC0262,60]]                        |2             |383         |191.5            |
  * |IND0003   |Sarah      |Skinner   |[[IND0003,ACC0235,631], [IND0003,ACC0486,400], [IND0003,ACC0540,53]] |3             |1084        |361.3333333333333|
  * ...
  */
object AccountAssessment extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  //Create DataFrames of sources
  val customerDF: DataFrame = spark.read.option("header","true")
    .csv("src/main/resources/customer_data.csv")
  val accountDF = spark.read.option("header","true")
    .csv("src/main/resources/account_data.csv")

  case class CustomerData(
                           customerId: String,
                           forename: String,
                           surname: String
                         )

  case class AccountData(
                          customerId: String,
                          accountId: String,
                          balance: Long
                        )

  //Expected Output Format
  case class CustomerAccountOutput(
                                    customerId: String,
                                    forename: String,
                                    surname: String,
                                    //Accounts for this customer
                                    accounts: Seq[AccountData],
                                    //Statistics of the accounts
                                    numberAccounts: BigInt,
                                    totalBalance: Long,
                                    averageBalance: Double
                                  )

  //Create Datasets of sources
  val customerDS: Dataset[CustomerData] = customerDF.as[CustomerData]
  val accountDS: Dataset[AccountData] = accountDF.withColumn("balance",'balance.cast("long")).as[AccountData]

  customerDS.show
  accountDS.show

  val accountOutput = accountDS.groupBy("customerId")
    .agg(collect_set(struct("customerId", "accountId", "balance")) as "accounts",
      count("accountId").cast("BigInt") as "numberAccounts",
      sum("balance").cast("long") as "totalBalance",
      avg("balance").cast("double") as "averageBalance")
  //Joining customerData and accountData
  val customerAccountOutputDS :Dataset[CustomerAccountOutput] = customerDS.join(accountOutput, Seq("customerId"), "left").na.fill(0)
    .sort(customerDS("customerId")).as[CustomerAccountOutput]
 //Displaying output

  customerAccountOutputDS.show(truncate = false)

  //customerAccountOutputDS.write.parquet("src/main/resources/customerAccountOutputDS.parquet")
  customerAccountOutputDS.write.mode("overwrite").parquet("src/main/resources/customerAccountOutputDS.parquet")
//END GIVEN CODE

}
