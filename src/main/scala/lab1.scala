import org.apache.log4j.{Level, Logger};
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import java.sql.Timestamp;
import scala.collection.mutable.WrappedArray;

case class GKGSchemaHeaders
(
	GKGRECORDID:				String,
	DATE:						Timestamp,
	SourceCollectionIdentifier:	Integer,
	SourceCommonName:			String,
	DocumentIdentifier:			String,
	Counts:						String,
	V2Counts:					String,
	Themes:						String,
	V2Themes:					String,
	Locations:					String,
	V2Locations:				String,
	Persons:					String,
	V2Persons:					String,
	Organizations:				String,
	V2Organizations:			String,
	V2Tone:						String,
	Dates:						String,
	GCAM:						String,
	SharingImage:				String,
	RelatedImages:				String,
	SocialImageEmbeds:			String,
	SocialVideoEmbeds:			String,
	Quotations:					String,
	AllNames:					String,
	Amounts:					String,
	TranslationInfo:			String,
	Extras:						String	
)

object Lab1
{
	def main(args: Array[String])
	{
		//	Initialize Spark
		println("Starting lab 1!");
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		val spark = SparkSession.builder.appName("ET4310 Lab 1").getOrCreate();
		val sc = spark.sparkContext;
		import spark.implicits._;

		// Define the GKG schema
		val GKGSchema = StructType(
			Seq(
					StructField("GKGRECORDID",					StringType,		true),
					StructField("DATE",							TimestampType,	true),
					StructField("SourceCollectionIdentifier",	IntegerType,	true),
					StructField("SourceCommonName",				StringType,		true),
					StructField("DocumentIdentifier",			StringType,		true),
					StructField("Counts",						StringType,		true),
					StructField("V2Counts",						StringType,		true),
					StructField("Themes",						StringType,		true),
					StructField("V2Themes",						StringType,		true),
					StructField("Locations",					StringType,		true),
					StructField("V2Locations",					StringType,		true),
					StructField("Persons",						StringType,		true),
					StructField("V2Persons",					StringType,		true),
					StructField("Organizations",				StringType,		true),
					StructField("V2Organizations",				StringType,		true),
					StructField("V2Tone",						StringType,		true),
					StructField("Dates",						StringType,		true),
					StructField("GCAM",							StringType,		true),
					StructField("SharingImage",					StringType,		true),
					StructField("RelatedImages",				StringType,		true),
					StructField("SocialImageEmbeds",			StringType,		true),
					StructField("SocialVideoEmbeds",			StringType,		true),
					StructField("Quotations",					StringType,		true),
					StructField("AllNames",						StringType,		true),
					StructField("Amounts",						StringType,		true),
					StructField("TranslationInfo",				StringType,		true),
					StructField("Extras",						StringType,		true)	
			)
		);
		
		// Read the files
		val segments = spark.read.schema(GKGSchema).
			option("delimiter", "\t").
			option("timestampFormat", "YYYYMMDDHHMMSS").
			csv("data/segment/*.csv").
			as[GKGSchemaHeaders];

		// UDF for removing the character offset number from each occurrence of a topic in a document
		val removeCharOffset = udf((data : String) => data.substring(0, data.lastIndexOf(',')));
		// UDF for converting a full date+timestamp to only the date
		val removeTimeComponent = udf((date : String) => date.substring(0, date.indexOf(' ')));
		// UDF for merging two columns into one column containing a tuple of the values of the original columns
		val mergeColumns = udf((col1 : String, col2 : String) => (col1, col2));
		// UDF for only selecting the first 10 elements of an array
		val shortenArray = udf((wrappedArray : WrappedArray[(String,String)]) => wrappedArray.take(10));

		// Find, count and group the topics per date
		val countedTopicsPerDay = segments.
			select("DATE", "AllNames").									//select the two relevant columns
			withColumn("DATE", removeTimeComponent($"DATE")).			//keep only the date part of this column
			withColumn("AllNames", explode(split($"AllNames", ";"))).	//generate a seperate row for each date-topic pair
			filter("AllNames != 'null'").								//remove the null entries
			withColumn("AllNames", removeCharOffset($"AllNames")).		//remove the character offset numbers from each row
			filter("AllNames != 'Type ParentCategory'").				//remove this false positive
			groupBy("DATE", "AllNames").count().						//group and count the occurrences of each topic
			orderBy(desc("count")).										//order them in a descending order by count
			withColumn("AllNames", mergeColumns($"AllNames", $"count")).//merge the AllNames and count column
			select("DATE", "AllNames").									//effectively delete the count column
			groupBy("DATE").agg(collect_list("AllNames").as("AllNames")).	//group by date
			withColumn("AllNames", shortenArray($"AllNames"));			//keep only the first ten topics per date

		// Print the result
		println("####################### DATASET IMPLEMENTATION #######################");
		countedTopicsPerDay.collect.foreach(println);

		println("####################### RDD IMPLEMENTATION #######################");

		def tupleconvert (array: Array[String]): (String, String) = (array(0),array(1))
		def toTuple(tuple: (String, String)): (String, Int) = try { tuple match{
			case (chr,int) => (chr,1)
			case _ => ("",0)}
			} catch {case e: Exception => ("",0)}
		def totalfunction (rdd: (String, String)): Array[(String, Int)] = {
			val rdd2 = rdd._2.split(";").filter(x => x != "" ).map(x => x.split(","))
			val rdd3 =  rdd2.map(tupleconvert).map(toTuple) //Array[(String,Int)]
			return rdd3.map(x => ( rdd._1+ " "+ x._1, x._2))}//.sortBy(_._2,false)
		
		val raw_data = sc.textFile("data/segment")
		val RDD1 = raw_data.map(_.split("\t",-1)).map(x => (x(0).substring(0, 8),x(23)))
		val RDD2 = RDD1.groupByKey().map(x => (x._1,x._2.toString())) 
		//val RDD3 = RDD2.flatMap(totalfunction).reduceByKey(_+_).sortBy(_._2,false)
		val RDD4 = RDD2.collectAsMap().map(x => (x._1,x._2.split(";").filter(x => x != "" )))
		val RDD5 = RDD4.map(x => (x._1,sc.parallelize(x._2).map(x => x.split(",")).map 
	(tupleconvert).map(toTuple).filter(x => x._1 != "Type ParentCategory").reduceByKey(_ + _).sortBy(_._2,false)))	
		RDD5.map(x => (println("date: "+x._1), x._2.take(10).foreach(println)))

		spark.stop();
	}
}
