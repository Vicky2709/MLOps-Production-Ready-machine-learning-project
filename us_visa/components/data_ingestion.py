import os
import sys

from pandas import DataFrame
from sklearn .model_selection import train_test_split

from us_visa.entity.config_entity import DataIngestionConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact
from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.data_access.usvisa_data import USVisaData


class DataIngestion:
    """
    Class Name :   DataIngestion
    Description :   This class is used to perform data ingestion from MongoDB database.
    
    Output      :   connection to mongodb database
    On Failure  :   raises an exception
    """

    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise USvisaException(e, sys)
        
    def export_data_into_feature_store(self) -> DataFrame:

        """
        Method Name :   export_data_into_feature_store
        Description :   This method exports the dataframe from mongodb feature store as dataframe 
        
        Output      :   dataframe
        On Failure  :   raises an exception
        """
        try:
            logging.info("Exporting data from MongoDB to feature store")
            usvisa_data = USVisaData()
            dataframe = usvisa_data.export_data_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            logging.info(f" shape of the dataframe: {dataframe.shape}")
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_name = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_name, exist_ok=True)
            logging.info(f"Exporting data to {feature_store_file_path}")
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe

        except Exception as e:
            raise USvisaException(e, sys)


    def split_data_as_train_test(self, dataframe: DataFrame) -> DataIngestionArtifact:
        """
        Method Name :   split_data_as_train_test
        Description :   This method splits the data into train and test data.
        
        Output      :   Folder is created in s3 bucket
        On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Entered split_data_as_train_test method of Data_Ingestion class")
        try:
            train_set, test_set = train_test_split(dataframe, test_size=self.data_ingestion_config.train_test_split_ratio,
                                                   random_state=42)
            logging.info("Performed train test split on the data")

            # Save the train and test data to csv files
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)

            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)

            logging.info(f"Exported train and test file path")
        except Exception as e:
            raise USvisaException(e, sys)
        
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Method Name :   initiate_data_ingestion
        Description :   This method initiates the data ingestion components of training pipeline 
        
        Output      :   train set and test set are returned as the artifacts of data ingestion components
        On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Entered initiate_data_ingestion method of Data_Ingestion class")

        try:
            dataframe = self.export_data_into_feature_store()
            logging.info("Exported data into feature store")
            self.split_data_as_train_test(dataframe=dataframe)
            logging.info("Split the data into train and test set")
            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            logging.info("Data ingestion artifact created")
            return data_ingestion_artifact
        except Exception as e:
            raise USvisaException(e, sys)
