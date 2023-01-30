from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[10]").appName("WordCount").getOrCreate()
# df = spark.read.text("gimme.txt")
sc = spark.sparkContext
words = sc.textFile('/tmp/uttam/sam_uttam.txt').flatMap(lambda line: line.split(' '))
a = words.map(lambda word: (word, 1))
a.collect()
