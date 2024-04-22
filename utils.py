# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# COMMAND ----------

def check_duplicates(df, column_name):
    """
    Check for duplicates in a DataFrame based on a specified column.
    
    Args:
    - df (DataFrame): The input DataFrame.
    - column_name (str): The column name to check for duplicates.
    
    Raises:
    - ValueError: If duplicates exist, raise an error.
    """
    # Count the number of distinct values and the total number of rows
    distinct_count = df.select(column_name).distinct().count()
    total_count = df.count()
    
    # If distinct count is less than total count, duplicates exist
    if distinct_count < total_count:
        # Find duplicate values
        duplicate_values = df.groupBy(column_name).count().filter(col("count") > 1)
        duplicate_values.show()
        
        # Raise error
        raise ValueError("Duplicates found in column: {}".format(column_name))
    else:
        return "No duplicates 👍"

# COMMAND ----------

class ForeignKeyConstraintError(Exception):
    pass

# COMMAND ----------

def check_foreign_key_constraint(parent_df: DataFrame, child_df: DataFrame, parent_column: str, child_column: str):
    """
    Check for foreign key constraint between two DataFrames.
    
    Args:
    - parent_df (DataFrame): DataFrame representing the parent table.
    - child_df (DataFrame): DataFrame representing the child table.
    - parent_column (str): Name of the column in the parent table.
    - child_column (str): Name of the column in the child table.
    
    Raises:
    - ForeignKeyConstraintError: If foreign key constraint is violated.
    """
    # Count the number of records in the child table where the value in child_column does not exist in parent_column
    invalid_records_df=child_df.join(parent_df, child_df[child_column] == parent_df[parent_column], "left_anti")
    invalid_records_count = invalid_records_df.count()
    
    if invalid_records_count == 0:
        print("Foreign key constraint is satisfied. 👍")
    else:
        error_msg = "Foreign key constraint violated: {} records in child table have invalid values in {} column".format(invalid_records_count, child_column)
        invalid_records_df.show()
        raise ForeignKeyConstraintError(error_msg)