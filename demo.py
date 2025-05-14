from us_visa.logger import logging
from us_visa.exception import USvisaException
import sys


# This is a demo script to show how to use the logger in your application.

logging.info("Welcome to the US Visa Application Assistant! This is the custom logger.")


#This is a demo script to show exception handling in your application.

try:
    a = 1 / 0
except Exception as e:
    raise USvisaException(e, sys)
