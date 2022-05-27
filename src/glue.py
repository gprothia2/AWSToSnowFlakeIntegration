import pandas as pd
import awswrangler as wr
import pandas_to_snowflake as ps
from datetime import datetime
import boto3
from extension import extension as ext
from constants import metadata_table, log_table, trigger_file, con   

def process_data(row,con):
    
## Get the  Metadata about dataset
  raw_data_loc  = row['LandingZoneLocation']
  staged_data_loc = row['StageZoneLocation']
  target_table_name = row['SnowflakeTable']
  target_table_pk = row['SnowflakePK']
  dataset_name = row['DatasetName']
  
  date_time = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
 
## Read the raw data 
  raw_data = wr.s3.read_csv(raw_data_loc)
  
## Call extension method. This is a hook through which client can implement custom transformations to data. Modified data is returned
  ext.custom(dataset_name,raw_data)

#Populating frame for upserting rows. Add 2 additional column for each row. LAST_UPDATE_DATE with value when the data was processed. DELETE_FLAG to mark row as soft deleted if the record indicator = D 
  upsert_rows = raw_data
  upsert_rows['DELETE_FLAG']=upsert_rows['ins_upd_del_flag'].apply(lambda x: 'N' if x != 'D' else 'Y')
  upsert_rows['LAST_UPDATE_DATE'] = date_time
  
## Remove the first column as it contains the insert/update/delete flag.
  upsert_rows = raw_data.iloc[:,1:len(upsert_rows.columns)]
  
## Stage A: Load all data into Staged Location in parquet format in S3. Before loading the data, perform de-deuplication to only keep the latest records
  
  stageA_start_time =  datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
  
  if len(wr.s3.list_objects(staged_data_loc)) >0 :
      df_existing = wr.s3.read_parquet(path=staged_data_loc)
  else:
      df_existing = upsert_rows 
      
  df_all = pd.concat([df_existing,upsert_rows])
    
  df_deduplicate =df_all.drop_duplicates(subset=[target_table_pk],keep='last')    
    
  wr.s3.to_parquet(
        df= df_deduplicate,index=False,dataset=True,mode="overwrite_partitions",
        path=staged_data_loc)  
        
  stageA_end_time =  datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    
  
## Stage B. Load the data to Snowflake tables 
  stageB_start_time =  datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
 
## Call the method sf_upsert from pandas_to_snowflake library to upser the row. This method acceptas a datafarem, connection details, schema, PK on which to perform the upsert and inserts/update rows to the target table
  ps.sf_upsert(dataframe=upsert_rows, snowflake_auth = con, schema = 'PUBLIC',table=target_table_name,upsertkey=(target_table_pk,))
  
  stageB_end_time =  datetime.today().strftime('%Y-%m-%d-%H:%M:%S')

  
## Write the run log to log table in Dybnamo DB. Calculate total record count, and also counts for  Insert/Update/Delete. This information alongwith start and end time for Stage A , Stage B will be saved in the log 
  
  total_count = raw_data.shape[0]
  counts = raw_data.value_counts('ins_upd_del_flag')
  update_count = counts.get('U')
  insert_count = counts.get('I')
  delete_count = counts.get('D')
  count_metric = '--Total:' + str(total_count) + ' --UpdateCount:' + str(update_count) + \
                ' --InsertCount:' + str(insert_count) + ' --DeleteCount:' + str(delete_count)
  file_list = ':'.join(wr.s3.list_objects(raw_data_loc))
  log_df=pd.DataFrame({'DatasetName': [dataset_name],  
                'DateTime': [date_time], 
                'stageA_begin_time': [stageA_start_time],
                'stageA_end_time': [stageA_end_time],
                'stageB_begin_time': [stageB_start_time],
                'stageB_end_time': [stageB_end_time],
                'Record_Count': count_metric,
                'Raw_files': file_list,
                'Run_status': 'SUCCESS'
               } )

  wr.dynamodb.put_df(df=log_df,table_name=log_table)


# Function to Archive the files that have been processed and delete the original files from LandingZoneLocation
def archive_files(row):
  
  raw_data_loc  = row['LandingZoneLocation']
  archive_data_loc  = row['ArchiveZoneLocation']
  raw_data_loc = raw_data_loc[:-1]
  
  wr.s3.copy_objects(paths=wr.s3.list_objects(raw_data_loc),
                     source_path=raw_data_loc,
                     target_path=archive_data_loc) 
  wr.s3.delete_objects(raw_data_loc)

# FUnction to establish connection to Snowflake database

def main():
    
# Initiate connection to metada table in dynamod db
  ddb = wr.dynamodb.get_table(metadata_table)

# Read Trigger file
  triggerdata = wr.s3.read_csv(trigger_file)

# Loop through list of datasets in the trigger file, get the metadata information 
  for index, row in triggerdata.iterrows():
      
    metadata = ddb.get_item(Key={'DatasetName':row[0]})
    row2=metadata['Item']
    
# Check if the LandingZoneLocation contains new files. If there are new files, call function process_data and archive_data to process the data 
    if len(wr.s3.list_objects(row2['LandingZoneLocation'])) >0:
      process_data(row2,con)
      archive_files(row2)
      
main()
