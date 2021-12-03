############################################################################################
############################ Welcome to Python Transforms ##################################
############################################################################################
#
# IMPORTANT: do not edit code on master. create a new branch by clicking the '+' icon in the 
# upper left corner. Any name is allowed, we recommend your username. 
#
# Your learnings in the PySpark SQL tutorial will be almost all you need to work with our 
# Python Transforms tool. This file is an example of how you can make your Notebook code into
# a permanent Data Pipeline. The @transform_df decorator should be the only unfamiliar concept 
# to you, as the rest is simply copied from our Notebook example (see the Notebook cell we 
# copied below). By following the steps in this folder (_1_basic_walkthrough), you will learn 
# about the extra concepts involved in Python Transforms, not present in Notebook. For now, 
# read this file and see how we have simply copied the Notebook code. 
# 
# Then, to see our dataset get built:
# 0) make sure you have created a new branch as warned above,
# 1) replace "USERNAME" on line 53 with your username,
# 2) hit "Commit" in the right-side toolbar above to run our code validation process,
# 3) click "Checks" in the left-side toolbar above to see the validation status
# 4) upon successful commit, click "Build" in the right-side toolbar,
# 5) go to the output folder (specified on line 53) to view your built dataset, which you can use
# across the Foundry platform (e.g. Contour analyses).  
#
############################################################################################
############ This is full content of the Notebook cell we have translated: #################
############################################################################################
# This code translates `6c. Basic Join in Practice` in the Foundry Transforms - SQL Tutorial
#
# This query finds the drivers who have taken the most trips from Manhattan to the Airport
#
# df = trips.join(
#     drivers,
#     trips.hack_license==drivers.hack_license
#     ).filter(
#         F.col('end_neighborhood').like('%Airport%') |
#         F.col('start_neighborhood').like('%Airport%')
#     ).groupBy(
#         drivers.name
#     ).agg(
#         F.count('*').alias('num_trips')
#     ).orderBy(
#         F.col('num_trips').desc()
#     )

#############################################################################################
#############################################################################################

from transforms.api import transform_df, Input, Output
from pyspark.sql.functions import col
import pyspark.sql.functions as F

@transform_df(
    Output("/Users/USERNAME/Python Transforms/X. Output Datasets/Notebook_to_Pytrans"),
    trips=Input("/datasources/taxi/raw/taxi_trips"),
    drivers=Input("/datasources/taxi/raw/driver_details")
)

def basicJoin(trips, drivers):
    df = trips.join(
            drivers,
            trips.hack_license==drivers.hack_license
        ).filter(
            F.col('end_neighborhood').like('%Airport%') |
            F.col('start_neighborhood').like('%Airport%')
        ).groupBy(
            drivers.name
        ).agg(
            F.count('*').alias('num_trips')
        ).orderBy(
            F.col('num_trips').desc()
        )
    return df
    
    # The below boilerplate imports basic file input, file output,
# and transformation functionality from Foundry Transforms.

from transforms.api import transform_df, Input, Output

# In the @transform_df decorator, you specify Output and Input paths. 
# No two transformations may output to the same path. If the output 
# path does not exist, the transform will create it. The output will
# be a dataset. The output path must be a child of the python repository's 
# parent directory (the parent directory of our python repository is 
# `/trainings/python/1. Python Transforms Tutorial: Basic Introduction`)

@transform_df(
    # Replace USERNAME with your username
    Output("/Users/lruigomez/Python Transforms/X. Output Datasets/meteor_duplicate"),
    
    # Input files must be assigned to keywords, which are used in the following compute function. To learn more about python decorator syntax, see https://realpython.com/blog/python/primer-on-python-decorators/
    meteorite_landings=Input("/datasources/meteorites/meteorite_landings")
)

# The @transform_df decorator "wraps" the compute function below. 
# The compute function contains the logic of your transformation;
# all Python and PySpark code is valid. The Input and Output 
# (reading and writing) are handled by the decorator.

def my_compute_function(meteorite_landings):
    # type: (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame
    return meteorite_landings.filter(
        meteorite_landings.nametype == 'Valid'
      ).filter(
        meteorite_landings.year >= 1950
      )


from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

# Next, we want to create a filtered dataset that only 
# includes meteors with greater-than-average mass(comparing
# each meteorite against other meteorites in the same class).

# In this file we will: find average mass for each meteorite type.

# Replace USERNAME with your username
@transform_df(
    Output("/Users/USERNAME/Python Transforms/X. Output Datasets/meteorite_stats"),
    meteorite_landings=Input("/datasources/meteorites/meteorite_landings"),
)

# Here we use pyspark.sql.DataFrame.groupBy and 
# pyspark.sql.DataFrame.agg. For more DataFrame 
# functions, see https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame 

# We also use pyspark.sql.functions (imported as F).
# For more on that topic, see 
# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
def stats(meteorite_landings):
    
    return meteorite_landings.groupBy("class").agg(
            F.mean("mass").alias("avg_mass_per_class")
    )

# In the next file we will compare each meteorite's mass to the 
# average mass for its meteorite type. Continue to _2b_SQL_like_operations
from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

# This transform ingests two input files. We compare each 
# meteorite's mass to the average mass for its meteorite type, 
# and only keep above-average meteorites.

@transform_df(
    Output("/Users/USERNAME/Python Transforms/X. Output Datasets/meteorite_above_avg"),
    meteorite_landings=Input("/datasources/meteorites/meteorite_landings"),
    meteorite_avg_mass=Input("/Users/USERNAME/Python Transforms/X. Output Datasets/meteorite_stats")
)

# Here we showcase pyspark.sql.DataFrame.join and 
# pyspark.sql.DataFrame.withColumn. For more DataFrame 
# functions, see https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame 
def enriched(meteorite_landings, meteorite_avg_mass):
    enriched_together=meteorite_landings.join(
        meteorite_avg_mass, "class"
    )
    greater_mass=enriched_together.withColumn(
        'greater_mass', (enriched_together.mass > enriched_together.avg_mass_per_class)
    )
    return greater_mass.filter("greater_mass")
    #############################################################################################
#############################################################################################
# One of python transforms' strengths over SQL transforms is its 
# ability to perform iterative logic. One powerful use case of iterative
# logic is to repeat logic to produce multiple output datasets.

# This file produces 3 transforms.api.Transform objects (our previous
# files produced only one each). They are stored in the TRANSFORMS variable
# as an array. 
#############################################################################################
#############################################################################################

#from transforms.api import transform_df, Input, Output

#def transform_generator(sources):
#    # type: (List[str]) -> List[transforms.api.Transform]
#    transforms = []
#    for source in sources:
#        @transform_df(
#            # This will create a different output dataset for each meteorite type
#            Output('/trainings/python/1. Python Transforms Tutorial: Basic Introduction/X. Output Datasets/USERNAME/meteorite_{source}'.format(source=source)),
#            my_input=Input('/trainings/python/1. Python Transforms Tutorial: Basic Introduction/X. Output Datasets/USERNAME/meteorite_above_avg')
#        )

        # We must pass  source=source into the filter_by_source function 
        # in order to capture the source parameter in the function's scope.
#        def filter_by_source(my_input, source=source):
#            return my_input.filter(my_input["class"] == source)
#
#        transforms.append(filter_by_source)
#
#    return transforms
# This will apply the data transformation logic from above to the three provided meteorite types
###TRANSFORMS = transform_generator(["L6", "H5", "H4"])

# IMPORTANT: every transform you produce must be discovered by your project's
# Pipeline. This was done automatically in previous files, which produced only one
# transform per file. When you produce multiple transforms from one file, you 
# must manually register the transforms to your Pipeline (or else the Pipeline will
# only discover the last transform produced in your iteration). Add the following code 
# to your pipeline.py:

# ...
# from .datasets import _3_Iteration
# ... 
# my_pipeline.add_transforms(*_3_Iteration.TRANSFORMS) # (* is python's argument unpacking operator)
from transforms.api import transform, Input, Output
from myproject._2_advanced_example.helper import generate_mappings, fix_csv

# This is an advanced use case of a python transform. It requires the @transform 
# decorator, which allows you to access raw files from the Foundry filesystem
# (because FileSystem objects are exposed by TransformInput and TransformOutput objects).
# See _3_optional_concepts/transform_decorator.py for a more thorough exposition to 
# the @transform decorator. 

# Use case:
# We have a dataset composed of two CSVs:
# 
# A.csv
# +---------------------------------------------+
# |Reval Date|Deal Number|comm opt exercised fla|
# +---------------------------------------------+
# |str       |int        |str                   |
# |str       |int        |str                   |
# +---------------------------------------------+
#
# B.csv
# +------------------------------------------------------------+
# |Deal Number|Reval_Type|comm opt exercised flag|Param_Seq_Num|
# +------------------------------------------------------------+
# |int        |str       |str                    |int          |
# +------------------------------------------------------------+
#
# As you can see, A and B share some column names, have typos in others, 
# and do not share ordering of columns. With python transforms, we can 
# address all of these discrepancies and produce an output dataset like this:
# +------------------------------------------------------------------------+
# |Reval_Date|Deal_Number|comm _opt_exercised_flag|Reval_Type|Param_Seq_Num|
# +------------------------------------------------------------------------+
# |str       |int        |str                     |None      |None         |
# |str       |int        |str                     |None      |None         |
# |None      |int        |str                     |str       |int          |
# +------------------------------------------------------------------------+
# 
# See the below compute function and the helper.py module
@transform(
    raw_dataset=Input("/Tutorials/06.0 Python and SQL Transforms/06.04 Python Transforms Tutorial/A. Additional Tutorial Datasets/broken_raw_dataset"),
    parsed_dataset=Output("/Users/USERNAME/Python Transforms/X. Output Datasets/fixed_dataset"),
)
def raw_bp_pnl_detail(raw_dataset, parsed_dataset, ctx):
    # the proper column names and order for output dataset
    schema = [
        "Reval_Date",
        "Deal_Number",
        "comm_opt_exercised_flag",
        "Reval_Type",
        "Param_Seq_Num"
    ]

    # second argument to mappings indicates that columns named KEY should be output as column name VALUE
    mappings = generate_mappings(schema, {"comm opt exercised fla": "comm opt exercised flag"})
    parsed_dataset.write_dataframe(fix_csv(raw_dataset, schema, mappings, ctx))

import csv
from pyspark.sql import Row
from pyspark.sql.types import *

# Given a correct ordering and spelling of columns (a canonical index), return a dict 
# which maps column names in the input CSVs to their canonical index. 
# e.g. 
# { ...
#  "comm opt exercised flag": 2
#   ...
#   "comm opt exercised fla": 2
#   ...
#  }
def generate_mappings(schema, mappings):
#   type: (List(str), Dict(str:str)) -> Dict(str:int)


    # Initialize the mapping with the information in schema
    result = dict((k, pos) for (pos, k) in enumerate(schema))

    # For every key with '_', create another key replacing with ' ', with same index value
    for col in schema:
        result[col.replace('_', ' ')] = result[col]

    # For every 'bad' key given in 'mappings', give it the right canonical index
    for key in mappings:
        result[key] = result[mappings[key]]

    return result

# Given a raw dataset and schema and mappings, return the correct output dataframe
def fix_csv(raw_dataset, schema, mappings, ctx):
#   type: (TransformInput, List(str), Dict(str:str), transforms.api.TransformContext) -> pyspark.sql.DataFrame
    def process_file(file_status):
        with raw_dataset.filesystem().open(file_status.path, 'rb') as f:
            r = csv.reader(f)

            # Construct a pyspark.Row from the header row
            header = next(r)
            SchemaRow = Row(*schema)

            for row in csv.reader(f):
                # We want to account for jagged rows, and we don't want to drop them
                output_row = [None for _ in schema]
                for (pos, cell) in enumerate(row):
                    # Find the column name in mappings
                    if header[pos] in mappings: 
                        # Assign the cell to the proper index in output_row
                        output_row[mappings[header[pos]]] = cell
                yield SchemaRow(*output_row)

    # Get all csv files associated with this raw_dataset. This returns a pyspark.sql.DataFrame
    files = raw_dataset.filesystem().files('**/*.csv')
    rdd = files.rdd.flatMap(process_file)
    # Specify a schema to pass into pyspark.sql.SparkSession.createDataFrame
    schema = StructType([
        StructField("Reval_date", StringType(), True),
        StructField("Deal_Number", StringType(), True),
        StructField("comm_opt_exercised_flag", StringType(), True),
        StructField("Reval_Type", StringType(), True),
        StructField("Param_Seq_Num", StringType(), True)])
    # ctx.spark_session is how you access the Spark session used to run the transform
    # ctx must be passed from the compute function, and the spelling must be exactly ctx
    return ctx.spark_session.createDataFrame(rdd, schema)
# If your transformation relies on information other than the input datasets, 
# inject a TransformContext object into the compute function by passing the 
# reserved-name parameter `ctx`. With the TransformContext object, you can access 
# the auth_header, fallbrack_branches, and spark_session for your particular transform job. 

from transforms.api import transform_df, Output
@transform_df(
    Output('/Users/USERNAME/Python Transforms/X. Output Datasets/ctx_createDataframe')
)
def generate_dataframe(ctx):
    # type: (TransformContext) -> pyspark.sql.DataFrame
    return ctx.spark_session.createDataFrame([
        ['a', 1], ['b', 2], ['c', 3]
    ], schema=['letter', 'number'])

# 2. See the advanced folder for an example of using ctx

# The @transform decorator allows certain functionality not available with 
# the @transform_df decorator. See the 'advanced' folder for an example.

# The @transform decorator can accept DataFrame objects or raw files as inputs. 
# Put simply, it takes care of reading and writing data from the Foundry filesystem.
# The @transform block is said to "wrap" the compute function that succeeds it. 
from transforms.api import transform, Input, Output

@transform(
    meteorite_landings=Input("/datasources/meteorites/meteorite_landings"),
    processed=Output("/Users/USERNAME/Python Transforms/X. Output Datasets/transform_decorator_filter"),
)

# The wrapped compute function must refer to the keyword arguments defined in the 
# @transform decorator, `meteorite_landings` and `processed`. Within the compute function, 
# all Python or PySpark syntax is valid. As you can see by the function's type specification
# below, the Input and Output objects from @transform are resolved to 
# TransformInput and TransformOutput objects.

# Whereas the @transform_df decorator automatically handles input and output,
# with the @transform decorator you must specify how to handle the 
# TransformInput and TransformOutput objects.
def clean(meteorite_landings, processed):
	# type: (TransformInput, TransformOutput) -> None
	df = meteorite_landings.dataframe()
	filtered_df = df.filter(
			df.nametype == 'Valid'
		).filter(
			df.year >= 1950
		)
	processed.write_dataframe(filtered_df)
# The @transform_df decorator injects a DataFrame and expects the 
# compute function to return a DataFrame. The @transform decorator
# injects the more powerful transforms.api.TransformInput and  
# transforms.api.TransformOutput objects, rather than DataFrame objects.
from transforms.api import transform_df, Input, Output

@transform_df(
    Output("/Users/USERNAME/Python Transforms/X. Output Datasets/transform_df_decorator_filter"),
    meteorite_landings=Input("/datasources/meteorites/meteorite_landings")
)

# The wrapped compute function must refer to the keyword arguments defined in the 
# @transform_df decorator, `meteorite_landings.` Within the compute function, 
# all Python or PySpark syntax is valid. 
def clean(meteorite_landings):
	# type: (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame
	return meteorite_landings.filter(
			meteorite_landings.nametype == 'Valid'
		).filter(
			meteorite_landings.year >= 1950
		)
    
 # This file defines your project's Pipeline. The Pipeline registers each Transform object 
# created by your *.py files. If your transformations are not producing datasets, the 
# problem might be in this file. Read /docs/python-transforms/1.9.0/user_guide/project_structure.html
# for more information. 

from transforms.api import Pipeline

from . import _1_basic_walkthrough
from . import _2_advanced_example
from . import _3_optional_concepts

#from ._1_basic_walkthrough import _5_iterate_for_multiple_outputs

my_pipeline = Pipeline()

# registering Transform objects in your "_1_basic_walkthrough" folder
my_pipeline.discover_transforms(_1_basic_walkthrough)

# registering Transform objects in your "_2_advanced_example" folder
my_pipeline.discover_transforms(_2_advanced_example)

# etc...
my_pipeline.discover_transforms(_3_optional_concepts)

# USER ADDS:
#my_pipeline.add_transforms(*_5_iterate_for_multiple_outputs.TRANSFORMS)
