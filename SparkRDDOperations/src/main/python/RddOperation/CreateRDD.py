from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("Python WordCount")
sc = SparkContext(conf=conf)
lines = sc.textFile("C:\app\web\logs\Messaging_Log\Messaging_Log.log.2018-04-04")
errorLines = lines.filter(lambda errorLine : "error" in errorLine)
errorLines.saveAsTextFile("E:\error.txt")