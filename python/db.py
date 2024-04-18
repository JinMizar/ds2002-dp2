from pymongo import MongoClient, errors
import os
import json


MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.asg2rg
# specify a collection
collection = db.myCollection

# Path to the JSON files
path = "data"

# Keep track of # of count imports
successful_imports = 0
failed_imports = 0
corrupted_files = 0

# Traversing through each file in the directory
for (root, dirs, files) in os.walk(path):
    for f in files:
        file_path = os.path.join(root, f)
        try:
            with open(file_path, 'r') as file:
                # Loading data from JSON file
                file_data = json.load(file)
            
            # Inserting data into the collection
            if isinstance(file_data, list):
                collection.insert_many(file_data)
                successful_imports += len(file_data)
            else:
                collection.insert_one(file_data)
                successful_imports += 1

        except json.JSONDecodeError:
            # Handling error
            corrupted_files += 1
        except Exception as e:
            # Handling other errors if exists
            failed_imports += 1

# Outputting import counts to a text file
with open('count.txt', 'w') as count_file:
    count_file.write(f"Records imported: {successful_imports}\n")
    count_file.write(f"Records orphaned (complete but not imported): {failed_imports}\n")
    count_file.write(f"Records corrupted: {corrupted_files}\n")

print(f"Data import completed. {successful_imports} successful, {failed_imports} failed, {corrupted_files} corrupted.")

