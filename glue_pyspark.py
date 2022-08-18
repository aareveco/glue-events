import ast
from collections import defaultdict
from pyspark.sql import Window
from pyspark.sql.functions import col, dense_rank, first

# Read from the events table in the glue data catalog using a dynamic frame
dynamicFrameEvents = glueContext.create_dynamic_frame.from_catalog(
database = "pyspark_tutorial_db", 
table_name = "events"
)

# Define user function to map records
def process_event(rec):
    if rec['messagetype'] == 'OrderConfirm':
        rec['order_number'] = ast.literal_eval(rec['data'])['orderInfo']['orderNumber']
        rec['is_confirmed'] = True
        rec['buyer']=rec['sender']
        rec['seller']=rec['receiver']
        rec['last_confirmed_time'] = rec['createdtime']
  
        del rec['data']
        del rec['sender']
        del rec['receiver']
 
    if rec['messagetype'] == 'OrderCreate':
        data = ast.literal_eval(rec['data'])
        rec['order_number'] = data['orderInfo']['orderNumber'].strip()
        rec['net_quantity'] = data['orderInfo']['quantityInfo']['netQuantity']
        rec['requested_delivery_date'] = data['dateInfo']['requestedDeliveryDate']
        rec['order_creation_time'] = rec['createdtime']
        rec['is_confirmed'] = False
        rec['buyer']=rec['receiver']
        rec['seller']=rec['sender']
        
        del rec['data']
        del rec['sender']
        del rec['receiver']
   
    if rec['messagetype'] == 'OrderChange':
        data = defaultdict(lambda: None, ast.literal_eval(rec['data']))
        rec['order_number'] = data['orderInfo']['orderNumber'].strip()
        if  data['orderInfo']:
            rec['net_quantity'] = data['orderInfo']['quantityInfo']['netQuantity']
        else:
             rec['net_quantity'] = None
        if data['dateInfo']:
            rec['requested_delivery_date'] = data['dateInfo']['requestedDeliveryDate']
        else:
            rec['requested_delivery_date'] = None
        rec['is_confirmed']=False
        rec['buyer']=rec['receiver']
        rec['seller']=rec['sender']
        
        del rec["data"]
        del rec['sender']
        del rec['receiver']
 
 
    return rec
dynamicFrameEvents.show(4)
# Change to a dataframe data structure and drop the field "id"
events_df =Map.apply(frame = dynamicFrameEvents, f = process_event).toDF().drop('id')

# Rename createdtime column into last_modified
events_df=events_df.withColumnRenamed("createdtime","last_modified")
events_df.show(4)

# the following part of the code group processed events by order_number and get the values from column not null with the last timestamp
window = (
    Window
    .partitionBy("order_number")
    .orderBy(col("last_modified").desc())
)
window_unbounded = (
    window
    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

cols_to_merge = [col for col in events_df.columns if col not in ["order_number", "last_modified"]]
merged_cols = [first(col, True).over(window_unbounded).alias(col) for col in cols_to_merge]
df_merged = (
    events_df
    .select([col("order_number"), col("last_modified")] + merged_cols)
    .withColumn("rank_col", dense_rank().over(window))
    .filter(col("rank_col") == 1)
    .drop("rank_col")
)
df_merged.drop('messagetype')
df_merged.show(4)
