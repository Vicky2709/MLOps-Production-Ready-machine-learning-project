import json
import sys

import pandas as pd
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection

from pandas import DataFrame

from us_visa.exception import USvisaException
from us_visa.logger import logging
from us_visa.utils.main_utils import read_yaml_file, write_yaml_file
from us_visa.entity.config_entity import DataValidationConfig
from us_visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from us_visa.constants import SCHEMA_FILE_PATH



class DataValidation:

    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        """
        :param data_ingestion_artifact: Output reference of data ingestion artifact stage
        :param data_validation_config: configuration for data validation
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config =read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise USvisaException(e, sys) from e
        
    
    def validate_num_of_columns(self, dataframe: DataFrame) -> bool:
        """
        Validate the number of columns in the dataframe against the schema file.
        :param dataframe: DataFrame to validate
        :return: True if the number of columns matches, False otherwise
        """
        try:
            
            status = len(dataframe.columns) == len(self._schema_config['columns'])
            logging.info(f"Is Number of columns present: {status}")
            return status
        except Exception as e:
            raise USvisaException(e, sys) from e
        
    def is_column_exists(self, dataframe: DataFrame) -> bool:
        """
        Method Name :   is_column_exist
        Description :   This method validates the existence of a numerical and categorical columns
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            dataframe_columns = dataframe.columns
            missing_numerical_columns = []
            missing_categorical_columns = []

            for column in self._schema_config['numerical_columns']:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)
            
            if len(missing_numerical_columns) > 0:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")
            
            for column in self._schema_config['categorical_columns']:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)

            if len(missing_categorical_columns) > 0:
                logging.info(f"Missing categorical columns: {missing_categorical_columns}")
            


            return False if len(missing_numerical_columns) > 0 or len(missing_categorical_columns) > 0 else True
                
        except Exception as e:
            raise USvisaException(e, sys) from e
        
    
    @staticmethod
    def read_file(file_path: str) -> DataFrame:
        """
        Method Name :   read_file
        Description :   This method reads the file from the given file path
        
        Output      :   Returns dataframe
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            dataframe = pd.read_csv(file_path)
            return dataframe
        except Exception as e:
            raise USvisaException(e, sys) from e
        

    def detect_dataset_drift(self, reference_df: DataFrame, current_df: DataFrame) -> bool:
        """
        Method Name :   detect_dataset_drift
        Description :   This method validates if drift is detected
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """

        try:
            data_drift_profile = Profile(sections=[DataDriftProfileSection()])

            data_drift_profile.calculate(reference_df, current_df)

            report = data_drift_profile.json()
            json_report = json.loads(report)

            write_yaml_file(file_path=self.data_validation_config.drift_report_file_path, content=json_report)    

            n_features = json_report["data_drift"]["data"]["metrics"]["n_features"]
            n_drifted_features = json_report["data_drift"]["data"]["metrics"]["n_drifted_features"]

            logging.info(f"{n_drifted_features}/{n_features} drift detected.")
            drift_status = json_report["data_drift"]["data"]["metrics"]["dataset_drift"]
            return drift_status
        except Exception as e:
            raise USvisaException(e, sys) from e
        


    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Method Name :   initiate_data_validation
        Description :   This method initiates the data validation component for the pipeline
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            validataion_error_msg = ""
            logging.info("Data Validation started")
            train_df, test_df = (DataValidation.read_file(file_path=self.data_ingestion_artifact.trained_file_path),
                                 DataValidation.read_file(file_path=self.data_ingestion_artifact.test_file_path))
            
            status = self.validate_num_of_columns(dataframe=train_df)
            logging.info(f"Is Number of columns present: {status}")
            if not status:
                validataion_error_msg += "Number of columns in the train file is not matching with schema\n"

            status = self.validate_num_of_columns(dataframe=test_df)
            logging.info(f"Is Number of columns present: {status}")
            if not status:
                validataion_error_msg += "Number of columns in the test file is not matching with schema\n"

            status = self.is_column_exists(dataframe=train_df)

            if not status:
                validataion_error_msg += "Numerical or Categorical columns are missing in the train file\n"

            status = self.is_column_exists(dataframe=test_df)
            if not status:
                validataion_error_msg += "Numerical or Categorical columns are missing in the test file\n"

            validation_status = len(validataion_error_msg) == 0

            if validation_status:
                drift_status = self.detect_dataset_drift(train_df, test_df)
                if drift_status:
                    logging.info("Drift detected in the dataset")
                    validataion_error_msg = "Drift detected in the dataset\n"
                else:
                    logging.info("No drift detected in the dataset")
                    validataion_error_msg = "No drift detected in the dataset\n"
            else:   
                logging.info(f"Data validation failed due to schema mismatch or missing columns : {validataion_error_msg}")
            
            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message= validataion_error_msg,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            logging.info(f"Data Validation Artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise USvisaException(e, sys) from e
        

    
        
        
    
        

