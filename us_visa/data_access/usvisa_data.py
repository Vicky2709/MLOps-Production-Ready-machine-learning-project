from us_visa.configuration.mongo_db_connection import MongoDBClient
from us_visa.constants import DATABASE_NAME
from us_visa.exception import USvisaException

import pandas as pd
import sys
from typing import Optional
import numpy as np


class USVisaData:
    """
    Class Name :   USVisaData
    Description :   This class is used to access the data from MongoDB database.
    
    Output      :   connection to mongodb database
    On Failure  :   raises an exception
    """

    def __init__(self):
        try:
            self.mongo_client = MongoDBClient(DATABASE_NAME)
        except Exception as e:
            raise USvisaException(e, sys)
        

    def export_data_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
            
        try: 
            """
            Method Name :   export_data_as_dataframe
            Description :   This method exports the dataframe from mongodb feature store as dataframe 
            
            Output      :   dataframe
            On Failure  :   raises an exception
            """
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client.client[database_name][collection_name]

            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.to_list():
                df.drop(columns=["_id"], axis=1, inplace=True)
            df.replace({np.nan: None}, inplace=True)
            return df
    
        except Exception as e:
            raise USvisaException(e, sys)