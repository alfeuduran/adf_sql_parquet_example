from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time

from six import string_types


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)

def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n\n")

def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print("\tNumber of bytes read: {}".format(activity_run.output['dataRead']))
        print("\tNumber of bytes written: {}".format(activity_run.output['dataWritten']))
        print("\tCopy duration: {}".format(activity_run.output['copyDuration']))
    else:
        print("\tErrors: {}".format(activity_run.error['message']))



def main():

    # Azure subscription ID
    subscription_id = ''
    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = ''
    # The data factory name. It must be globally unique.
    df_name = ''
    # Specify your Active Directory client ID, client secret, and tenant ID
    client_id = ''
    client_secret = ''
    tenant_id = ''
    

    credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

    #SQL Linked Service Name from Data Factory
    ls_sql_name = ''
    
    # Storage Linked Service name from Data Factory
    ls_storage_name = '' 

    # Table name 
    table_name = ''

    #Table Schema Example!

    table_schema = [{'name': 'AddressID','type': 'int','precision': 10},
    {"name": "AddressLine1","type": "nvarchar"},
    {"name": "AddressLine2","type": "nvarchar"},
    {"name": "City","type": "nvarchar"},
    {"name": "StateProvince","type": "nvarchar"},
    {"name": "CountryRegion","type": "nvarchar"},
    {"name": "PostalCode","type": "nvarchar"},
    {"name": "rowguid","type": "uniqueidentifier"},
    {"name": "ModifiedDate","type": "datetime","precision": 23,"scale": 3}]

    
    # Inharits the Data Factory object
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    # Name for SQL Dataset (Input)
    ds_sql_name = ''
    ds_ls = LinkedServiceReference(reference_name=ls_sql_name)
    ds_sql = DatasetResource(properties=AzureSqlTableDataset(linked_service_name=ds_ls,table_name=table_name,schema=table_schema))
    #Create or update a exisintg one
    adf_client.datasets.create_or_update(rg_name,df_name,ds_sql_name,ds_sql)
    

    #Name for Output parquet dataset
    ds_Out_name = ''
    ls_storage_name = ''
    ds_ls = LinkedServiceReference(reference_name= ls_storage_name)
    output_blobpath = 'container/folder'
    blob_file_name = 'filename.parquet'
    parquet_format = ParquetFormat()
    
    #Create or update a exiting one (dataset)
    dsOut_azure_blob = DatasetResource(properties=AzureBlobDataset(linked_service_name=ds_ls,folder_path=output_blobpath,file_name=blob_file_name,format=parquet_format))
    ds_Out = adf_client.datasets.create_or_update(rg_name,df_name,ds_Out_name,dsOut_azure_blob)
    

    #Create Pipeline Copy Activity
    act_name = '' #Activity Name
    sqlSource = AzureSqlSource()
    blob_sink = BlobSink()

    ds_sql_ref = DatasetReference(reference_name=ds_sql_name) #take the input dataset name
    ds_storage_ref = DatasetReference(reference_name=ds_Out_name) # take the output dataset name

    # Create the Data Factory Activity
    copyActivity = CopyActivity(name=act_name,inputs=[ds_sql_ref],outputs=[ds_storage_ref],source=sqlSource,sink=blob_sink)

    
    #Define the pipeline name
    p_name = ''
    params_for_pipeline = {} #if existis put the parameters configuration here
    p_obj = PipelineResource(activities=[copyActivity], parameters=params_for_pipeline)
    #Create or update a existing one
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj) 
    print_item(p)

    
    ####### Run the Pipeline ######
    
    #Create Pipeline Run
    run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})


     #Monitor a pipeline run
    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        rg_name, df_name, pipeline_run.run_id, filter_params)
    print_activity_run_details(query_response.value[0])


main()