#!/usr/bin/env python

import yaml
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

# create dataframe
df = sqlContext.createDataFrame([
    ("Mary", 15),
    ("John", 18),
    ("Alex", 30),
], ["name", "age"])

# read rules from yaml file
# - 'age > 15 or name != "Mary"'
# - 'name != "Alex"'
with open('test.yaml', 'rb') as f:
    rules = yaml.load(f)

# apply filters
for rule in rules:
    df = df.filter(rule)

print df.collect()
