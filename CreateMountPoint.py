# Databricks notebook source
def create_azure_blob_mount(mount_name):
    """
    Create a mount point to Azure Blob Storage if it does not already exist.
    
    Args:
    - mount_name (str): Name of the mount point.
    """
    # Check if mount point already exists
    mounts = dbutils.fs.mounts()
    existing_mounts = [mount.mountPoint for mount in mounts]
    
    if f"/mnt/{mount_name}" in existing_mounts:
        print(f"Mount point '{mount_name}' already exists. Skipping mount creation.")
    else:
        # Create mount point
        dbutils.fs.mount(
            source='wasbs://assignmentcontainer1@blobcontainerassignment.blob.core.windows.net/data',
            mount_point= f"/mnt/{mount_name}",
            extra_configs={'fs.azure.account.key.blobcontainerassignment.blob.core.windows.net': 'private key'}
        )
        print(f"Mount point '{mount_name}' created successfully.")

# COMMAND ----------

# Create mount point
create_azure_blob_mount("gsynergy")