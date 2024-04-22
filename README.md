"# GSynergy-assignment" 

->Created the ER diagram for showing raw table structure and relationships between the Dim tables and fact tables.

->Used blob storage to store the data.

->Then mounted that data into Azure databricks using Account key and created Notebook named CreateMountPoint .

->Created the utils notebook for the basic check like duplicated and foreign key constraint between two DataFrames.

->Created a database for staging named as CreateStagingDB and Processed all the dim and fact files and then put all processed files in folder named Staging.

->Created a database for Mart named as CreateMartDB and processed the final required data as result and stored that final result in mview_weekly_sales table.

->And Created 3 Pipeline in Azure Data Factory.

1. Staging
2. Mart
3. Master 

->And Published all the pipeline and click on debug in the Master Pipeline and It is giving the Activity status as Succeeded.

And Final Processed data is stored in mview_weekly_sales table.