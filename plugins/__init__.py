from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

# Explicitly import your packages from the local directory
from operators import (StageToRedshiftOperator, LoadFactOperator, 
                       LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from helpers import SqlQueries

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator,
        CreateTableOperator
    ]
    helpers = [
        SqlQueries
    ]
