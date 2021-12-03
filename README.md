# AIRBUSCODES

> Note: Click the triple dots below the `Advanced` dropdown in the upper right and click `Toggle Markdown`
 
 
### Basic Python Transforms Tutorial

DO THIS NOW: click the (+) button above to create your own sandbox branch. Its name 
can be your username. Then, create a folder within 
`/trainings/python/1. Python Transforms Tutorial: Basic Introduction/X. Output Datasets` and rename it to
your username. Within that folder, create two empty datasets, and name them
`meteorite_stats` and `meteorite_above_avg` respectively. 

----------------------------------------------------------------------------------------

Python transforms are more powerful than SQL transforms in three ways:

1) Python allows iterative logic. Iteration is a powerful programming technique
which, among other things, will let you repeat similar transformation logic without 
duplicating code. 

2) Unlike SQL transforms in Foundry, Python transforms allow you to access the 
Foundry filesystem, so you can manipulate files below the Foundry DataSet level. 
An example of this is shown in the `advanced` folder. 

3) Python has many libraries for statistical analysis. Any package or technique allowed
in Python is allowed in our Python transforms tool. 

----------------------------------------------------------------------------------------

1. If you are unfamiliar with pyspark SQL, go through the [PySpark SQL Tutorial](https://regalia.palantircloud.com/workspace/octavius/views/notebook/ri.octavius.main.notebook.1b97eaa4-80b7-4cbd-99e6-ef092d44469c).

2. Go in order through the files in `_1_basic_walkthrough`. You will
substitute your username in the indicated areas, and click "Build." Read the
comments for explanation of code.

3. See the `_2_advanced_example` folder for an in-depth use case of the concepts we learn 
in this tutorial. 

4. To learn more about the @transform decorator, @transform_df decorator, and the 
transforms.api.TransformContext object, see the `_3_optional_concepts` folder.

----------------------------------------------------------------------------------------

### FAQ:

1. Any new directory you create must contain a blank `__init__.py` file
And, you must add the following code to your pipeline.py file:
    ...
    from . import FOLDERNAME
    ... 
    my_pipeline.discover_transforms(FOLDERNAME)

Don't put periods in filenames, it will cause pain when you need to reference the file

2. Transforms can only write to files within this repository's parent directory. This 
repository's parent directory is "/trainings/python/1. Python Transforms Tutorial: Basic Introduction"

