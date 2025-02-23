import pandas as pd
import pymongo
import schedule
import time

# MongoDB configuration
# MongoDB configuration
mongo_client = pymongo.MongoClient("TYPE YOUR CONNECTION ID HERE")
db = mongo_client["TYPE DATABASE NAME HERE"]
collection = db["TYPE COLEECTION NAME HERE"]
control_collection = db["TYPE COLLECTION NAME HERE"]  # Collection to store last processed row index

# Function to get the last processed index from MongoDB
def get_last_processed_index():
    record = control_collection.find_one({"_id": "last_processed_row"})
    return record["index"] if record else 0  # Start from 0 if no record exists

# Function to update the last processed index in MongoDB
def update_last_processed_index(last_index):
    control_collection.update_one({"_id": "last_processed_row"}, {"$set": {"index": last_index}}, upsert=True)

# Function to perform incremental extraction from CSV and insert new rows into MongoDB
def batch_extract_and_insert(file_path, batch_size):
    # Get the last processed row index
    start_index = get_last_processed_index()
    
    # Read new data from the CSV file starting from the last processed row index
    df = pd.read_csv(file_path, skiprows=range(1, start_index + 1), nrows=batch_size)
    print(df.head)
    
    # Check if there are new rows to process
    if not df.empty:
        # Convert DataFrame to dictionary for MongoDB insertion
        data = df.to_dict(orient="records")
        collection.insert_many(data)  # Insert new rows into MongoDB
        
        # Update the last processed row index
        new_last_index = start_index + len(df)
        update_last_processed_index(new_last_index)
        
        print(f"Inserted batch from row {start_index} to {new_last_index - 1}")
    else:
        print("No new data to insert.")

# Wrapper function to schedule incremental extraction and insertion
def loader_scheduler(file_path, batch_size):
    # Schedule the job at a specified interval (e.g., every minute)
    schedule.every(10).seconds.do(lambda: batch_extract_and_insert(file_path, batch_size))
    
    # Keep running the scheduler
    while schedule.get_jobs():
        schedule.run_pending()
        time.sleep(1)

# Example usage
file_path = "type file path here"

batch_size = 5 
loader_scheduler(file_path, batch_size)
