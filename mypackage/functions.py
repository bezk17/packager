from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
from pyspark.sql import SparkSession
ss = SparkSession.getActiveSession()

data = [('0000010005678','10005678','2384690','827.6328973'),
        ("00000056720310",'56720310','jhfsi54w68r','12.3894723'),
       ("000000010024583",'10024583','sghod698','5463.910181190089'),
       ("0000001006273",'1006273','547386582','98.142674846828')]

columns = ["LeadStringValue","PadStringValue","DeterString","ID"]

rdd = ss.sparkContext.parallelize(data)

df = ss.createDataFrame(rdd).toDF(*columns)

def suppressLeadingZeroes (str):
    match = re.search("^(0+)(\d+)$", str)
    output = match.group(2) if match else str
    return output

def padLeadingZeroes(max_length,str):
    output = '0'*(max_length-len(str)) + str
    return output


def determineString(str) :
    return str.isnumeric()
    print(df.select("DeterString", determineUdf("DeterString").alias("String_determined")))


from pyspark.sql.functions import round, col
from pyspark.sql.functions import split
import pyspark

def split_number(df,col):
    df.select("*", round("col", 4)).show()
    df1 = df.withColumn("ID_String", df[col].cast(StringType()))
    str = df1['ID_String']
    split_col = pyspark.sql.functions.split(str, '\\.')
    df12 = df1.withColumn('Mantissa', split(str, '\\.')[0])
    df12 = df12.withColumn('Decimal', split_col[1])
    df12.show()
    df12.agg(max(length(col("Mantissa")))).show()
    df12.agg(max(length(col("Decimal")))).show()


def matchRegex(df,col,pattern) :
    import re
    ans = ""
    split_pat = pyspark.sql.functions.split(pattern, '\\**')
    for i in split_pat :
        matched = re.match(r"split_pat",split_pat[i],col)
        output = True if matched else False
        if(output==True) :
            break
        else :
            continue
    for i in split_pat:
        df.withColumn("col_regex".format[i], matchRegex(df,"PadStringValue", split_pat)).show()
    return 0

def valueCheck(var,list):
    result = var.isin(list)
    return result

def cons(ss):
    sparksuppressUdf = udf(lambda x: suppressLeadingZeroes(x), StringType())
    ss.udf.register(sparksuppressUdf)
    padZeroUdf = udf(lambda y: padLeadingZeroes(y), StringType())
    ss.udf.register(padZeroUdf)
    determineUdf = udf(determineString, StringType())
    ss.udf.register(determineUdf)
    splitUDF = udf(split_number(), StringType())
    ss.udf.register(splitUDF)
    matchUdf = udf(matchRegex(), StringType())
    ss.udf.register(matchUdf)
    valueCheckUdf = udf(valueCheck(), StringType())
    ss.udf.register(valueCheckUdf)