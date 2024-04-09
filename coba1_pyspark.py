from pyspark import SparkConf, SparkContext
import collections

# Check if a SparkContext instance already exists
try:
    sc = SparkContext.getOrCreate()
except Exception as e:
    print("Error:", e)
    raise SystemExit(1)

# Now you can proceed with your Spark operations
lines = sc.textFile("file:///bigdata/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
