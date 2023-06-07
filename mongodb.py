from configparser import ConfigParser

from pymongo import MongoClient
import os

DB_NAME = 'company_eval'

def get_mongodb_client():
    """
    Get mongodb client
    :return: mongodb client
    """

    # Get credentials
    parser = ConfigParser()
    _ = parser.read(os.path.join("credentials.cfg"))
    username = parser.get("mongo_db", "username")
    password = parser.get("mongo_db", "password")

    # Set connection string
    LOCAL_CONNECTION = "mongodb://localhost:27017"
    ATLAS_CONNECTION = f"mongodb+srv://{username}:{password}@cluster0.3dxfmjo.mongodb.net/?" \
                       f"retryWrites=true&w=majority"
    ATLAS_OLD_CONNECTION = f"mongodb://{username}:{password}@cluster0.3dxfmjo.mongodb.net:27017/?" \
                          f"retryWrites=true&w=majority&tls=true"
    # print(ATLAS_CONNECTION)

    connection_string = LOCAL_CONNECTION

    # Create a connection using MongoClient
    client = MongoClient(connection_string)

    return client

def get_collection(collection_name):
    db = get_mongodb_client()[DB_NAME]
    return db[collection_name]

def get_file_size(file_name):
    file_stats = os.stat(file_name)
    print(f'File Size in Bytes is {file_stats.st_size}')
    return file_stats.st_size

def get_dict_size(data):
    import sys
    print("The size of the dictionary is {} bytes".format(sys.getsizeof(data)))
    return sys.getsizeof(data)

def upsert_document(collection_name, data):
    collection = get_collection(collection_name)
    collection.replace_one({"_id":data["_id"]}, data, upsert=True)

def insert_document(collection_name, data):
    collection = get_collection(collection_name)
    collection.insert_one(data)

def get_document(collection_name, document_id):
    collection = get_collection(collection_name)
    return collection.find({"_id": document_id}).next()

def check_document_exists(collection_name, document_id):
    collection = get_collection(collection_name)
    return collection.count_documents({"_id": document_id}, limit=1) > 0

def get_collection_documents(collection_name):
    collection = get_collection(collection_name)
    return collection.find({})