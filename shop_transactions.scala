import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField,StructType,StringType,IntegerType,TimestampType}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date


//val df = sc.textFile("/user/6387305bac3bed857be0e9c4793574/transactions.csv")
val df = sc.textFile("/user/hduser/trans/transactions")

//val dfarrmap = df.map(x  => x.split(","))

//dfarrmap.foreach(x => print(x.mkString("|=|")))

//val dfmap = df.map(x  => Row(x.split(",")))

val format = new java.text.SimpleDateFormat("MM-dd-yyyy")

val rowRDD = df.map(_.split(",")).map(e => Row(e(0).trim.toString, new Timestamp(format.parse(e(1).trim.toString).getTime()), 
                                            e(2).trim.toString, e(3).trim.toInt, e(4).trim.toString, e(5).trim.toString, e(6).trim.toString))
val customSchema = StructType(Array(
    StructField("customer_id", StringType, true),
    StructField("date", TimestampType, true),
    StructField("transaction_id", StringType, true),
    StructField("trans_cost", IntegerType, true),
    StructField("product", StringType, true),
    StructField("city", StringType, true),
    StructField("trans_type", StringType, true)))

val dfsql =  sqlContext.createDataFrame(rowRDD, customSchema)

dfsql.groupBy($"trans_type").avg().toDF("transaction type", "Avg Transaction").show


