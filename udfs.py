from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

def convert_date(x):
    date = datetime.strptime(str(x), '%Y%m%d').strftime('%m/%d/%Y')
    return date

def is_tornado(val):
    # grab last digit and filter values such as 10, 101 etc. which doesn't follow the guide!!
    if int(str(val)[-1]) == 1 and len(str(val)) == 6:
        return True
    else:
        return None

convert_date_udf = udf(lambda z: convert_date(z), StringType())
is_tornado_udf = udf(lambda z: is_tornado(z), BooleanType())

