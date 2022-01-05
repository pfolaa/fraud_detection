import os,json
import pandas as pd
import glob2
import glob
import tqdm
import regex
import re
import json
import datetime
import numpy as np
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split

from regex_preproc import type_request_dictionnary
from utils import convertToTimestamp , read_json_insert_csv,  process_json, parse_wallet_sms_payload_success, parse_providus_transfer_error_function, parse_and_concatenate_Leadway_Success_Rows, parse_Leadway_Success_Row, parse_Error_Row, parse_Error_Row_selfie_Function, parse_Error_Row_selfie_INSERT_INTO_Function, parse_Error_Row_Function 
from utils import convertToTimestamp
from utils import read_json_insert_csv
from utils import process_json
from utils import getListFileCSV
from utils import parse_wallet_sms_payload_success
from utils import parse_providus_transfer_error_function
from utils import parse_and_concatenate_Leadway_Success_Rows
from utils import parse_Leadway_Success_Row
from utils import parse_Error_Row
from utils import parse_Error_Row_selfie_Function
from utils import parse_Error_Row_selfie_INSERT_INTO_Function
from utils import parse_Error_Row_Function
from utils import df_number_of_operations_per_minute
from utils import list_all_df_with_conversion_in_datetime
from utils import get_all_df_diff_ts


# row level functions

def parse_ERROR_DF(df_):
    
  ''' la fonction permet de parser les types de requete "ERROR" en object json.
    Elle prend en paramètre le text contenu dans le type de requete,
    elle retourne un objet de type JSON.'''  
    
  log_level_col = []
  type_request_col = []
  error_code_col = []
  error_number_col = []
  error_sql_message_col = []
  error_sql_state_col = []
  error_index_col = []
  error_sql_col = []
  loan_amount_col = []
  loan_purpose_col = []
  product_col = []
  tenor_col = []
  tenor_type_col = []
  error_created_by_col = []
  error_creator_type_col = []
  error_date_created_col = []
  error_name_col = []
  error_rate_col = []
  error_request_col = []
  error_status_col = []
  error_userID_col = []
  date_col = []
  
  list_all_colum = []
  list_all_colum = [type_request_col, log_level_col, error_code_col, error_number_col, error_sql_message_col, error_sql_state_col, 
                    error_index_col, error_sql_col, loan_amount_col, loan_purpose_col, product_col, tenor_col, tenor_type_col,
                    error_created_by_col, error_creator_type_col, error_date_created_col, error_name_col, error_rate_col,
                    error_request_col, error_status_col, error_userID_col, date_col]

  for index, row in df_.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)
    
    if re.search('error', str_text):
      log_level = re.search('error', str_text)
      type_of_request = re.search('LOAN ERROR', str_text)
      date_col.append(row['ts'])

      if 'INSERT INTO' not in str_text:  
          try:
            log_level_col.append(log_level.group(0))
          except AttributeError:
            log_level_col.append(None)
          try:
            type_request_col.append(type_of_request.group(0))
          except AttributeError:
            type_request_col.append(None)         
          try:
            loan_error = parse_Error_Row_selfie_Function(str_text) 
          except json.decoder.JSONDecodeError:
            raise          
          try:
            loan_amount_col.append(loan_error.get('loan_amount'))           
          except AttributeError:
            loan_amount_col.append(None)
          try:
            loan_purpose_col.append(loan_error.get('loan_purpose'))              
          except AttributeError:
            loan_purpose_col.append(None)
          try:
            product_col.append(loan_error.get('product'))
          except AttributeError:
            product_col.append(None)
          try:
            tenor_col.append(loan_error.get('tenor'))
          except AttributeError:
            tenor_col.append(None)
          try:
            tenor_type_col.append(loan_error.get('tenor_type'))
          except AttributeError:
            tenor_type_col.append(None)            
          
          error_code_col.append(None)
          error_number_col.append(None)
          error_sql_message_col.append(None)
          error_sql_state_col.append(None)
          error_index_col.append(None)
          error_sql_col.append(None)  
          error_userID_col.append(None)
          error_status_col.append(None) 
          error_request_col.append(None)  
          error_rate_col.append(None)
          error_name_col.append(None)
          error_date_created_col.append(None)
          error_creator_type_col.append(None)
          error_created_by_col.append(None)    
      else:     
        try:
          loan_error = parse_Error_Row_selfie_INSERT_INTO_Function(str_text) 
          #print(loan_error)
        except json.decoder.JSONDecodeError:
          raise
        loan_amount_col.append(None)
        loan_purpose_col.append(None)
        product_col.append(None)
        tenor_col.append(None)
        tenor_type_col.append(None)
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)
        try:
          type_request_col.append(type_of_request.group(0))
        except AttributeError:
          type_request_col.append(None)
        try:
          error_code_col.append(loan_error.get('code'))
        except AttributeError:
          error_code_col.append(None)
        try:
          error_number_col.append(loan_error.get('errno'))
        except AttributeError:
          error_number_col.append(None)
        try:
          error_sql_message_col.append(loan_error.get('sqlMessage'))
        except AttributeError:
          error_sql_message_col.append(None)
        try:
          error_sql_state_col.append(loan_error.get('sqlState'))
        except AttributeError:
          error_sql_state_col.append(None)
        try:
          error_created_by_col.append(loan_error.get('created_by'))
        except AttributeError:
          error_created_by_col.append(None)
        try:
          error_creator_type_col.append(loan_error.get('creator_type'))
        except AttributeError:
          error_creator_type_col.append(None)
        try:
          error_date_created_col.append(loan_error.get('date_created'))
        except AttributeError:
          error_date_created_col.append(None)
        try:
          error_sql_col.append(loan_error.get('sql'))
        except AttributeError:
          error_sql_col.append(None)
        try:
          error_name_col.append(loan_error.get('name'))
        except AttributeError:
          error_name_col.append(None)
        try:
          error_rate_col.append(loan_error.get('rate'))
        except AttributeError:
          error_rate_col.append(None)
        try:
          error_index_col.append(loan_error.get('index'))
        except AttributeError:
          error_index_col.append(None)
        try:
          error_request_col.append(loan_error.get('request'))
        except AttributeError:
          error_request_col.append(None)
        try:
          error_status_col.append(loan_error.get('status'))
        except AttributeError:
          error_status_col.append(None)
        try:
          error_userID_col.append(loan_error.get('userID'))
        except AttributeError:
          error_userID_col.append(None)

  df_['Type_Request'] = type_request_col
  df_['Date'] = date_col
  df_['Log_Level'] = log_level_col
  df_['Error Code'] = error_code_col
  df_['Error Number'] = error_number_col
  df_['Error Sql Message'] = error_sql_message_col
  df_['Error Sql State'] = error_sql_state_col
  df_['Error Index'] = error_index_col
  df_['Error Sql'] = error_sql_col
  df_['Loan Amount'] = loan_amount_col
  df_['Loan Purpose'] = loan_purpose_col
  df_['Product'] = product_col
  df_['Tenor'] = tenor_col
  df_['Tenor Type'] = tenor_type_col
  df_['Error created by'] = error_created_by_col
  df_['Error creator Type'] = error_creator_type_col 
  df_['Error Date created'] = error_date_created_col
  df_['Error Name'] = error_name_col
  df_['Error Rate'] = error_rate_col
  df_['Error Request'] = error_request_col
  df_['Error Status'] = error_status_col
  df_['Error UserID'] = error_userID_col


  return df_
  
  
  
def parse_row_LOAN_PAYLOAD(df_loan_payload):

  log_level_col = []
  type_request_col = []
  loan_amount_col = []
  loan_purpose_col = []
  product_col = []
  tenor_col = []
  tenor_type_col = []
  date_col = []


  list_all_colum = []
  list_all_colum = [type_request_col, log_level_col, loan_amount_col, loan_purpose_col, product_col, tenor_col, tenor_type_col, date_col]
          
  for index, row in df_loan_payload.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)
    
    if re.search('info', str_text):
        log_level = re.search('info', str_text)
        type_of_request = re.search('LOAN PAYLOAD', str_text)
        date_col.append(row['ts'])

        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)
        try:
          type_request_col.append(type_of_request.group(0))
        except AttributeError:
          type_request_col.append(None)         
        try:
          loan_error = parse_Error_Row_selfie_Function(str_text) 
        except json.decoder.JSONDecodeError:
          raise 
        try:
            loan_amount_col.append(loan_error.get('loan_amount'))           
        except AttributeError:
            loan_amount_col.append(None)
        try:
          loan_purpose_col.append(loan_error.get('loan_purpose'))              
        except AttributeError:
          loan_purpose_col.append(None)
        try:
          product_col.append(loan_error.get('product'))
        except AttributeError:
          product_col.append(None)
        try:
          tenor_col.append(loan_error.get('tenor'))
        except AttributeError:
          tenor_col.append(None)
        try:
          tenor_type_col.append(loan_error.get('tenor_type'))
        except AttributeError:
          tenor_type_col.append(None)  

  
  df_loan_payload['Type_Request'] = type_request_col
  df_loan_payload['Log_Level'] = log_level_col
  df_loan_payload['Loan Amount'] = loan_amount_col
  df_loan_payload['Loan Purpose'] = loan_purpose_col
  df_loan_payload['Product'] = product_col
  df_loan_payload['Tenor'] = tenor_col
  df_loan_payload['Tenor Type'] = tenor_type_col
  df_loan_payload['Date'] = date_col

  return df_loan_payload



def parse_API_REQUEST_DF(df_api_request):

  '''
    la fonction permet de parser les types de requete "API REQUEST" sur tout le dataframe
  '''
  
  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_col = []
  date_col = []
  endpoint_col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []

  list_column_none_api_request = []
  list_column_none_api_request = [message_sms_payload_col, totalsent_col, cost_col, status_col,
                                  bvn_col, requestSuccessful_col, responseMessage_col,
                                  responseCode_col, account_name_col, account_number_col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

  for index, row in df_api_request.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        log_level = re.search('info', str_text)
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)        
        # check if the row contains an email address 
        # pour tous les types request créer un dictionnaire dans lequel mapper
        # key = type de request et value = les regex définis
        # pour chaque condition IF créer une liste de colonnes auxquelles affecter None
        if 'mailto' in str_text:
            if re.search('API REQUEST', str_text):
                type_of_request = re.search('API REQUEST', str_text)                
                phone_or_email_api_req = re.search(type_request_dictionnary['API REQUEST'][0], str_text)                              
                endpoint = re.search(type_request_dictionnary['API REQUEST'][1], str_text)
                pattern = type_request_dictionnary['API REQUEST'][2]
                datepattern = re.compile("(?:%s)"%(pattern))
                datematcher = datepattern.search(str_text)  # extract date

                for i in range(len(list_column_none_api_request)):
                  list_column_none_api_request[i].append(None)
                               
                try:
                  type_request_col.append(type_of_request.group(0)) # add type request inside type request column
                except AttributeError:
                  type_request_col.append(None)               
                try:
                  email_col.append(phone_or_email_api_req.group(0)) # add email inside email column
                  phone_col.append(None)  # in this case there is no phone number
                except AttributeError:
                  email_col.append(None)
                try:
                  endpoint_col.append(endpoint.group(0)) # add endpoint inside endpoint column
                except AttributeError:
                  endpoint_col.append(None)
                try:
                  date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                except AttributeError:
                  date_col.append(None)
              

        elif 'mailto' not in str_text:
            if re.search('API REQUEST', str_text):
                type_of_request = re.search('API REQUEST', str_text)                            
                # extract a phone number for API REQUEST
                phone_or_email_api_req = re.search(type_request_dictionnary['API REQUEST'][3], str_text)                              
                endpoint = re.search(type_request_dictionnary['API REQUEST'][1], str_text)
                pattern = type_request_dictionnary['API REQUEST'][2]
                datepattern = re.compile("(?:%s)"%(pattern))
                datematcher = datepattern.search(str_text)  # extract date

                for i in range(len(list_column_none_api_request)):
                  list_column_none_api_request[i].append(None)

                try:
                  if len(phone_or_email_api_req.group(0)) > 4:
                      phone_col.append(phone_or_email_api_req.group(0)) # add phone number inside phone number column
                      email_col.append(None) # in this case there is no email address
                  else:
                      phone_col.append(None)
                      email_col.append(None)
                except AttributeError:
                  phone_col.append(None)
                try:
                  type_request_col.append(type_of_request.group(0))
                except AttributeError:
                  type_request_col.append(None)
                try:
                  endpoint_col.append(endpoint.group(0)) # add endpoint inside endpoint column
                except AttributeError:
                  endpoint_col.append(None)
                try:
                  date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                except AttributeError:
                  date_col.append(None)

  df_api_request['Type_Request'] = type_request_col
  df_api_request['Phone_Number'] = phone_col
  df_api_request['Date'] = date_col
  df_api_request['EndPoint'] = endpoint_col
  df_api_request['Log_Level'] = log_level_col
  df_api_request['Email'] = email_col
  df_api_request['Message SMS Payload'] = message_sms_payload_col
  df_api_request['Total Sent'] = totalsent_col
  df_api_request['Cost'] = cost_col
  df_api_request['Status'] = status_col
  df_api_request['Account Number'] = account_number_col
  df_api_request['Account Name'] = account_name_col
  df_api_request['BVN'] = bvn_col
  df_api_request['Request Successful'] = requestSuccessful_col
  df_api_request['Response Message'] = responseMessage_col
  df_api_request['Response Code'] = responseCode_col
 
  return df_api_request
  
  
def parse_CLIENT_MOBILE_LOGIN_DF(df_client_mob):

  '''
    la fonction permet de parser les types de requete "CLIENT MOBILE" sur tout le dataframe
  '''

  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_col = []
  date_col = []
  endpoint_Col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []

  list_column_none_client_mobile = []
  list_column_none_client_mobile = [message_sms_payload_col, totalsent_col, cost_col, status_col,
                                  account_number_col, bvn_col, requestSuccessful_col, responseMessage_col,
                                  responseCode_col, account_name_col, endpoint_Col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

  for index, row in df_client_mob.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        log_level = re.search('info', str_text)
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)        
        # check if the row contains an email address 
        # pour tous les types request créer un dictionnaire dans lequel mapper
        # key = type de request et value = les regex définis
        # pour chaque condition IF créer une liste de colonnes auxquelles affecter None
        if 'mailto' in str_text:
            if re.search('CLIENT MOBILE LOGIN', str_text):   # CLIENT MOBILE LOGIN with email address
                  type_of_request = re.search('CLIENT MOBILE LOGIN', str_text)
                  # extract address email for CLIENT MOBILE LOGIN
                  phone_or_email_client_mobile = re.search(type_request_dictionnary['CLIENT MOBILE LOGIN'][0], str_text)                               
                  pattern = type_request_dictionnary['CLIENT MOBILE LOGIN'][1]
                  datepattern = re.compile("(?:%s)"%(pattern))
                  datematcher = datepattern.search(str_text)  # extract date for CLIENT MOBILE LOGIN type request
                  
                  for j in range(len(list_column_none_client_mobile)):
                    list_column_none_client_mobile[j].append(None)

                  try:
                    type_request_col.append(type_of_request.group(0)) # add type request inside type request column
                  except AttributeError:
                    type_request_col.append(None) 
                  try:
                    email_col.append(phone_or_email_client_mobile.group(0)) # add email inside email column
                    phone_col.append(None)  # in this case there is no phone number
                  except AttributeError:
                    email_col.append(None)
                  try:
                    date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                  except AttributeError:
                    date_col.append(None)

        elif 'mailto' not in str_text:
            if re.search('CLIENT MOBILE LOGIN', str_text): # when type request is CLIENT MOBILE LOGIN, there is no EndPoint
                type_of_request = re.search('CLIENT MOBILE LOGIN', str_text)
                # extract a phone number for CLIENT MOBILE LOGIN
                phone_or_email_client_mobile = re.search(type_request_dictionnary['CLIENT MOBILE LOGIN'][2], str_text)                  
                pattern = type_request_dictionnary['CLIENT MOBILE LOGIN'][1]
                datepattern = re.compile("(?:%s)"%(pattern))
                datematcher = datepattern.search(str_text)  # extract date

                for j in range(len(list_column_none_client_mobile)):
                    list_column_none_client_mobile[j].append(None)

                try:
                  phone_col.append(phone_or_email_client_mobile.group(0))
                  email_col.append(None)
                except AttributeError:
                  phone_col.append(None)
                try:
                  type_request_col.append(type_of_request.group(0))
                except AttributeError:
                  type_request_col.append(None)
                try:
                  date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                except AttributeError:
                  date_col.append(None) 

  df_client_mob['Type_Request'] = type_request_col
  df_client_mob['Phone_Number'] = phone_col
  df_client_mob['Date'] = date_col
  df_client_mob['EndPoint'] = endpoint_Col
  df_client_mob['Log_Level'] = log_level_col
  df_client_mob['Email'] = email_col
  df_client_mob['Message SMS Payload'] = message_sms_payload_col
  df_client_mob['Total Sent'] = totalsent_col
  df_client_mob['Cost'] = cost_col
  df_client_mob['Status'] = status_col
  df_client_mob['Account Number'] = account_number_col
  df_client_mob['Account Name'] = account_name_col
  df_client_mob['BVN'] = bvn_col
  df_client_mob['Request Successful'] = requestSuccessful_col
  df_client_mob['Response Message'] = responseMessage_col
  df_client_mob['Response Code'] = responseCode_col
 
  return df_client_mob


def parse_SMS_PAYLOAD_DF(df_sms_payload):

  '''
    la fonction permet de parser les types de requete "SMS PAYLOAD" sur tout le dataframe
  '''
    
  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_col = []
  date_col = []
  endpoint_Col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []
  
  list_column_none_sms_payload = []
  list_column_none_sms_payload = [totalsent_col, cost_col, status_col,
                                  account_number_col, bvn_col, requestSuccessful_col, responseMessage_col,
                                  responseCode_col, account_name_col, email_col, endpoint_Col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

  for index, row in df_sms_payload.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        log_level = re.search('info', str_text)
        date_col.append(row['ts'])
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)             
        if 'mailto' not in str_text:
            if re.search('SMS PAYLOAD', str_text):
                type_of_request = re.search('SMS PAYLOAD', str_text)            
                sms_payload = parse_wallet_sms_payload_success(str_text)               
                for l in range(len(list_column_none_sms_payload)):
                    list_column_none_sms_payload[l].append(None)             
                try:
                  type_request_col.append(type_of_request.group(0))
                except AttributeError:
                  type_request_col.append(None)
                try:
                  phone_col.append(sms_payload.get('phone'))
                except AttributeError:
                  phone_col.append(None)
                try:
                  message_sms_payload_col.append(sms_payload.get('message'))
                except AttributeError:
                  message_sms_payload_col.append(None)


  df_sms_payload['Type_Request'] = type_request_col
  df_sms_payload['Phone_Number'] = phone_col
  df_sms_payload['Date'] = date_col
  df_sms_payload['EndPoint'] = endpoint_Col
  df_sms_payload['Log_Level'] = log_level_col
  df_sms_payload['Email'] = email_col
  df_sms_payload['Message SMS Payload'] = message_sms_payload_col
  df_sms_payload['Total Sent'] = totalsent_col
  df_sms_payload['Cost'] = cost_col
  df_sms_payload['Status'] = status_col
  df_sms_payload['Account Number'] = account_number_col
  df_sms_payload['Account Name'] = account_name_col
  df_sms_payload['BVN'] = bvn_col
  df_sms_payload['Request Successful'] = requestSuccessful_col
  df_sms_payload['Response Message'] = responseMessage_col
  df_sms_payload['Response Code'] = responseCode_col
 
  return df_sms_payload

  

def parse_SMS_SUCCESS_DF(df_sms_success):
    
  '''
    la fonction permet de parser les types de requete "SMS SUCCESS" sur tout le dataframe
   '''
   
  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_Col = []
  date_col = []
  endpoint_Col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []
  
  list_column_none_sms_success = []
  list_column_none_sms_success = [message_sms_payload_col, account_number_col, bvn_col, requestSuccessful_col, 
                                  responseMessage_col, responseCode_col, account_name_col, email_col, 
                                  phone_Col, endpoint_Col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_Col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

  for index, row in df_sms_success.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        log_level = re.search('info', str_text)
        date_col.append(row['ts'])
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)             
        if 'mailto' not in str_text:
            if re.search('SMS SUCCESS', str_text): 
                type_of_request = re.search('SMS SUCCESS', str_text)
                sms_success = parse_wallet_sms_payload_success(str_text)                
                for m in range(len(list_column_none_sms_success)):
                    list_column_none_sms_success[m].append(None) 
           
                try:
                  type_request_col.append(type_of_request.group(0))
                except AttributeError:
                  type_request_col.append(None)
                try:                 
                  totalsent_col.append(sms_success.get('response').get('totalsent '))
                except AttributeError:
                  totalsent_col.append(None)
                try:                 
                  cost_col.append(sms_success.get('response').get('cost '))
                except AttributeError:
                  cost_col.append(None)
                try:                 
                  status_col.append(sms_success.get('response').get('status '))
                except AttributeError:
                  status_col.append(None)
                     
        elif re.search('OKRA PAYLOAD', str_text): # Nothing
          type_of_request = re.search('OKRA PAYLOAD', str_text)
        elif re.search('OKRA SUCCESS', str_text):   # Nothing
          type_of_request = re.search('OKRA SUCCESS', str_text)
        elif re.search('VTPASS SUCCESS', str_text):   # Nothing
          type_of_request = re.search('VTPASS SUCCESS', str_text)    

  df_sms_success['Type_Request'] = type_request_col
  df_sms_success['Phone_Number'] = phone_Col
  df_sms_success['Date'] = date_col
  df_sms_success['EndPoint'] = endpoint_Col
  df_sms_success['Log_Level'] = log_level_col
  df_sms_success['Email'] = email_col
  df_sms_success['Message SMS Payload'] = message_sms_payload_col
  df_sms_success['Total Sent'] = totalsent_col
  df_sms_success['Cost'] = cost_col
  df_sms_success['Status'] = status_col
  df_sms_success['Account Number'] = account_number_col
  df_sms_success['Account Name'] = account_name_col
  df_sms_success['BVN'] = bvn_col
  df_sms_success['Request Successful'] = requestSuccessful_col
  df_sms_success['Response Message'] = responseMessage_col
  df_sms_success['Response Code'] = responseCode_col
 
  return df_sms_success


def parse_WALLET_SUCCESS_DF(df_wallet_success):

  '''
    la fonction permet de parser les types de requete "WALLET SUCCESS" sur tout le dataframe
   '''  
    
  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_col = []
  date_col = []
  endpoint_Col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []
  message_error_col = []

  list_column_none_wallet_success = []
  list_column_none_wallet_success = [totalsent_col, message_sms_payload_col, cost_col, status_col, 
                                     email_col, endpoint_Col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_name_col, bvn_col, 
                    requestSuccessful_col, responseMessage_col, responseCode_col]
  
  for index, row in df_wallet_success.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        log_level = re.search('info', str_text)
        date_col.append(row['ts'])
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)

        if re.search('WALLET SUCCESS', str_text):
            type_of_request = re.search('WALLET SUCCESS', str_text)           

            if 'Faithfully yours, nginx' in str_text:
                error_text = 'Sorry, the page you are looking for is currently unavailable, Please try again later.'
                message_error_col.append(error_text)
                type_request_col.append(type_of_request.group(0))
                account_name_col.append(None)
                bvn_col.append(None)
                requestSuccessful_col.append(None)
                responseMessage_col.append(None)
                responseCode_col.append(None)
                phone_col.append(None)
            else:
                try:
                  wallet_success = parse_providus_transfer_error_function(str_text)
                except json.decoder.JSONDecodeError:
                  #print(str_text)
                  raise
                message_error_col.append(None)
                try:
                  type_request_col.append(type_of_request.group(0))
                except AttributeError:
                  type_request_col.append(None)
                try:
                  phone_col.append(wallet_success.get('account_number'))
                except AttributeError:
                  phone_col.append(None)
                try:
                  account_name_col.append(wallet_success.get('account_name'))
                except AttributeError:
                  account_name_col.append(None)
                try:
                  bvn_col.append(wallet_success.get('bvn'))
                except AttributeError:
                  bvn_col.append(None)
                try:
                  requestSuccessful_col.append(wallet_success.get('requestSuccessful'))
                except AttributeError:
                  requestSuccessful_col.append(None)
                try:
                  responseMessage_col.append((wallet_success.get('responseMessage')))
                except AttributeError:
                  responseMessage_col.append(None)
                try:
                  responseCode_col.append((wallet_success.get('responseCode')))
                except AttributeError:
                  responseCode_col.append(None)

            for n in range(len(list_column_none_wallet_success)):
              list_column_none_wallet_success[n].append(None) 

                       

  df_wallet_success['Type_Request'] = type_request_col
  df_wallet_success['Phone_Number'] = phone_col
  df_wallet_success['Date'] = date_col
  df_wallet_success['EndPoint'] = endpoint_Col
  df_wallet_success['Log_Level'] = log_level_col
  df_wallet_success['Email'] = email_col
  df_wallet_success['Message SMS Payload'] = message_sms_payload_col
  df_wallet_success['Total Sent'] = totalsent_col
  df_wallet_success['Cost'] = cost_col
  df_wallet_success['Status'] = status_col
  df_wallet_success['Account Name'] = account_name_col
  df_wallet_success['BVN'] = bvn_col
  df_wallet_success['Request Successful'] = requestSuccessful_col
  df_wallet_success['Response Message'] = responseMessage_col
  df_wallet_success['Response Code'] = responseCode_col
  df_wallet_success['Error Message Wallet'] = message_error_col
 
  return df_wallet_success
  
  
def parse_OKRA_WEBHOOK_DF(df_okra):

  '''
    la fonction permet de parser les types de requete "OKRA WEBHOOK" sur tout le dataframe
   ''' 
   
  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_col = []
  date_col = []
  endpoint_Col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []
  accountId_col = []
  authorization_v_col = []
  authorization_id_col = []
  authorization_customer_col = []
  authorization_account_col = []
  authorization_account_id_col = []
  authorization_account_manual_col = []
  authorization_account_name_col = []
  authorization_account_nuban_col = []
  authorization_account_bank_col = []
  authorization_account_created_at_col = []
  authorization_account_last_updated_col = []
  authorization_account_balance_col = []
  authorization_account_customer_col = []
  authorization_account_type_col = []
  authorization_account_currency_col = []
  authorization_accounts_col = []
  authorization_amount_col = []
  authorization_bank_col = []
  authorization_created_at_col = []
  authorization_currency_col = []
  authorization_customerDetails_col = []
  authorization_disconnect_col = []
  authorization_disconnected_at_col = []
  authorization_duration_col = []
  authorization_env_col = []
  authorization_garnish_col = []
  authorization_initialAmount_col = []
  authorization_initiated_col = []
  authorization_last_updated_col = []
  authorization_link_col = []
  authorization_next_payment_col = []
  authorization_owner_col = []
  authorization_payLink_col = []
  authorization_type_col = []
  authorization_used_col = []
  authorizationId_col = []
  bankId_col = []
  bankName_col = []
  bankSlug_col = []
  bankType_col = []
  callbackURL_col = []
  callback_code_col = []
  callback_type_col = []
  callback_url_col = []
  code_col = []
  country_col = []
  current_project_col = []
  customerEmail_col = []
  customerId_col = []
  ended_at_col = []
  env_col = []
  extras_col = []
  identityType_col = []
  login_type_col = []
  message_col = []
  meta_col = []
  method_col = []
  options_col = []
  owner_col = []
  record_col = []
  recordId_col = []
  started_at_col = []
  status_webhook_col = []
  token_col = []
  type_col = []

  list_column_none_okra_webhook = []
  list_column_none_okra_webhook = [api_request_col, account_number_col, account_name_col, totalsent_col, 
                                   message_sms_payload_col, cost_col, status_col, responseCode_col,
                                   bvn_col, requestSuccessful_col, responseMessage_col, email_col, 
                                   endpoint_Col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

  for index, row in df_okra.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        date_col.append(row['ts'])
        log_level = re.search('info', str_text)
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)             
        if 'mailto' not in str_text:
            if re.search('OKRA WEBHOOK', str_text):
                  type_of_request = re.search('OKRA WEBHOOK', str_text)
                  okra_webhook = parse_wallet_sms_payload_success(str_text) 
                  if 'authorization' in str_text:
                      accountId_col.append(okra_webhook.get('accountId'))
                      authorization_v_col.append(okra_webhook.get('authorization').get('__v '))
                      authorization_id_col.append(okra_webhook.get('authorization').get('_id '))
                      authorization_customer_col.append(okra_webhook.get('authorization').get('customer '))
                      authorization_account_col.append(okra_webhook.get('authorization').get('account '))
                      authorization_account_id_col.append(okra_webhook.get('authorization').get('account ')[0].get('_id '))
                      authorization_account_manual_col.append(okra_webhook.get('authorization').get('account ')[0].get('manual '))
                      authorization_account_name_col.append(okra_webhook.get('authorization').get('account ')[0].get('name '))
                      authorization_account_nuban_col.append(okra_webhook.get('authorization').get('account ')[0].get('nuban '))
                      authorization_account_bank_col.append(okra_webhook.get('authorization').get('account ')[0].get('bank '))
                      authorization_account_created_at_col.append(okra_webhook.get('authorization').get('account ')[0].get('created_at '))
                      authorization_account_last_updated_col.append(okra_webhook.get('authorization').get('account ')[0].get('last_updated '))
                      authorization_account_balance_col.append(okra_webhook.get('authorization').get('account ')[0].get('balance '))
                      authorization_account_customer_col.append(okra_webhook.get('authorization').get('account ')[0].get('customer '))
                      authorization_account_type_col.append(okra_webhook.get('authorization').get('account ')[0].get('type '))
                      authorization_account_currency_col.append(okra_webhook.get('authorization').get('account ')[0].get('currency '))
                      authorization_accounts_col.append(okra_webhook.get('authorization').get('accounts '))
                      authorization_amount_col.append(okra_webhook.get('authorization').get('amount '))
                      authorization_bank_col.append(okra_webhook.get('authorization').get('bank '))
                      authorization_created_at_col.append(okra_webhook.get('authorization').get('created_at '))
                      authorization_currency_col.append(okra_webhook.get('authorization').get('currency '))
                      authorization_customerDetails_col.append(okra_webhook.get('authorization').get('customerDetails '))
                      authorization_disconnect_col.append(okra_webhook.get('authorization').get('disconnect '))
                      authorization_disconnected_at_col.append(okra_webhook.get('authorization').get('disconnected_at '))
                      authorization_duration_col.append(okra_webhook.get('authorization').get('duration '))
                      authorization_env_col.append(okra_webhook.get('authorization').get('env '))
                      authorization_garnish_col.append(okra_webhook.get('authorization').get('garnish '))
                      authorization_initialAmount_col.append(okra_webhook.get('authorization').get('initialAmount '))
                      authorization_initiated_col.append(okra_webhook.get('authorization').get('initiated '))
                      authorization_last_updated_col.append(okra_webhook.get('authorization').get('last_updated '))
                      authorization_link_col.append(okra_webhook.get('authorization').get('link '))
                      authorization_next_payment_col.append(okra_webhook.get('authorization').get('next_payment '))
                      authorization_owner_col.append(okra_webhook.get('authorization').get('owner '))
                      authorization_payLink_col.append(okra_webhook.get('authorization').get('payLink '))
                      authorization_type_col.append(okra_webhook.get('authorization').get('type '))
                      authorization_used_col.append(okra_webhook.get('authorization').get('used '))
                      authorizationId_col.append(None)
                      bankId_col.append(None)
                      bankName_col.append(None)
                      bankSlug_col.append(None)
                      bankType_col.append(None)
                      callbackURL_col.append(None)
                      callback_code_col.append(None)
                      callback_type_col.append(None)
                      callback_url_col.append(None)
                      code_col.append(None)
                      country_col.append(None)
                      current_project_col.append(None)
                      customerEmail_col.append(None)
                      customerId_col.append(None)
                      ended_at_col.append(None)
                      env_col.append(None)
                      extras_col.append(None)
                      identityType_col.append(None)
                      login_type_col.append(None)
                      message_col.append(None)
                      meta_col.append(None)
                      method_col.append(None)
                      options_col.append(None)
                      owner_col.append(None)
                      record_col.append(None)
                      recordId_col.append(None)
                      started_at_col.append(None)
                      status_webhook_col.append(None)
                      token_col.append(None)
                      type_col.append(None)
                      phone_col.append(None)
                      try:
                        type_request_col.append(type_of_request.group(0))
                      except AttributeError:
                        type_request_col.append(None)
                  else:
                      authorizationId_col.append(okra_webhook.get('authorizationId'))
                      bankId_col.append(okra_webhook.get('bankId'))
                      bankName_col.append(okra_webhook.get('bankName'))
                      bankSlug_col.append(okra_webhook.get('bankSlug'))
                      bankType_col.append(okra_webhook.get('bankType'))
                      callbackURL_col.append(okra_webhook.get('callbackURL'))
                      callback_code_col.append(okra_webhook.get('callback_code'))
                      callback_type_col.append(okra_webhook.get('callback_type'))
                      callback_url_col.append(okra_webhook.get('callback_url'))
                      code_col.append(okra_webhook.get('code'))
                      country_col.append(okra_webhook.get('country'))
                      current_project_col.append(okra_webhook.get('current_project'))
                      customerEmail_col.append(okra_webhook.get('customerEmail'))
                      customerId_col.append(okra_webhook.get('customerId'))
                      ended_at_col.append(okra_webhook.get('ended_at'))
                      env_col.append(okra_webhook.get('env'))
                      extras_col.append(okra_webhook.get('extras'))
                      identityType_col.append(okra_webhook.get('identityType'))
                      login_type_col.append(okra_webhook.get('login_type'))
                      message_col.append(okra_webhook.get('message'))
                      meta_col.append(okra_webhook.get('meta'))
                      method_col.append(okra_webhook.get('method'))
                      options_col.append(okra_webhook.get('options'))
                      owner_col.append(okra_webhook.get('owner'))
                      record_col.append(okra_webhook.get('record'))
                      recordId_col.append(okra_webhook.get('recordId'))
                      started_at_col.append(okra_webhook.get('started_at'))
                      status_webhook_col.append(okra_webhook.get('status'))
                      token_col.append(okra_webhook.get('token'))
                      type_col.append(okra_webhook.get('type'))
                      if okra_webhook.get('identity') != None:
                        try:
                          phone_col.append(okra_webhook.get('identity').get('phone ')[0])
                        except AttributeError:
                          phone_col.append(None)
                          #print(okra_webhook)
                          raise
                      else :
                        phone_col.append(None)
                        
                      accountId_col.append(None)
                      authorization_v_col.append(None)
                      authorization_id_col.append(None)
                      authorization_customer_col.append(None)
                      authorization_account_col.append(None)
                      authorization_account_id_col.append(None)
                      authorization_account_manual_col.append(None)
                      authorization_account_name_col.append(None)
                      authorization_account_nuban_col.append(None)
                      authorization_account_bank_col.append(None)
                      authorization_account_created_at_col.append(None)
                      authorization_account_last_updated_col.append(None)
                      authorization_account_balance_col.append(None)
                      authorization_account_customer_col.append(None)
                      authorization_account_type_col.append(None)
                      authorization_account_currency_col.append(None)
                      authorization_accounts_col.append(None)
                      authorization_amount_col.append(None)
                      authorization_bank_col.append(None)
                      authorization_created_at_col.append(None)
                      authorization_currency_col.append(None)
                      authorization_customerDetails_col.append(None)
                      authorization_disconnect_col.append(None)
                      authorization_disconnected_at_col.append(None)
                      authorization_duration_col.append(None)
                      authorization_env_col.append(None)
                      authorization_garnish_col.append(None)
                      authorization_initialAmount_col.append(None)
                      authorization_initiated_col.append(None)
                      authorization_last_updated_col.append(None)
                      authorization_link_col.append(None)
                      authorization_next_payment_col.append(None)
                      authorization_owner_col.append(None)
                      authorization_payLink_col.append(None)
                      authorization_type_col.append(None)
                      authorization_used_col.append(None)
                      try:
                        type_request_col.append(type_of_request.group(0))
                      except AttributeError:
                        type_request_col.append(None)
                  
                  for n in range(len(list_column_none_okra_webhook)):
                    list_column_none_okra_webhook[n].append(None) 

       
  df_okra['Type_Request'] = type_request_col
  df_okra['Phone_Number'] = phone_col
  df_okra['Date'] = date_col
  df_okra['EndPoint'] = endpoint_Col
  df_okra['Log_Level'] = log_level_col
  df_okra['Email'] = email_col
  df_okra['Message SMS Payload'] = message_sms_payload_col
  df_okra['Total Sent'] = totalsent_col
  df_okra['Cost'] = cost_col
  df_okra['Status'] = status_col
  df_okra['Account Number'] = account_number_col
  df_okra['Account Name'] = account_name_col
  df_okra['BVN'] = bvn_col
  df_okra['Request Successful'] = requestSuccessful_col
  df_okra['Response Message'] = responseMessage_col
  df_okra['Response Code'] = responseCode_col
  df_okra['Account Id'] = accountId_col
  df_okra['Authorization_V'] = authorization_v_col
  df_okra['Authorization_Id'] = authorization_id_col
  df_okra['Authorization_Customer'] = authorization_customer_col
  df_okra['Authorization_Owner'] = authorization_owner_col
  df_okra['Authorization_Account'] = authorization_account_col
  df_okra['Authorization_account_Id'] = authorization_account_id_col
  df_okra['Authorization_account_manual'] = authorization_account_manual_col
  df_okra['Authorization_account_name'] = authorization_account_name_col
  df_okra['Authorization_account_nuban'] = authorization_account_nuban_col
  df_okra['Authorization_account_bank'] = authorization_account_bank_col
  df_okra['Authorization_account_created_at'] = authorization_account_created_at_col
  df_okra['Authorization_account_last_updated'] = authorization_account_last_updated_col
  df_okra['Authorization_account_balance'] = authorization_account_balance_col
  df_okra['Authorization_account_customer'] = authorization_account_customer_col
  df_okra['Authorization_account_type'] = authorization_account_type_col
  df_okra['Authorization_account_currency'] = authorization_account_currency_col
  df_okra['Authorization_accounts'] = authorization_accounts_col
  df_okra['Authorization_amount'] = authorization_amount_col
  df_okra['Authorization_bank'] = authorization_bank_col
  df_okra['Authorization_created_at'] = authorization_created_at_col
  df_okra['Authorization_currency'] = authorization_currency_col 
  df_okra['Authorization_customerDetails'] = authorization_customerDetails_col
  df_okra['Authorization_disconnect'] = authorization_disconnect_col
  df_okra['Authorization_disconnected_at'] = authorization_disconnected_at_col
  df_okra['Authorization_duration'] = authorization_duration_col
  df_okra['Authorization_env'] = authorization_env_col
  df_okra['Authorization_garnish'] = authorization_garnish_col
  df_okra['Authorization_initialAmount'] = authorization_initialAmount_col
  df_okra['Authorization_initiated'] = authorization_initiated_col
  df_okra['Authorization_last_updated'] = authorization_last_updated_col
  df_okra['Authorization_link'] = authorization_link_col
  df_okra['Authorization_next_payment'] = authorization_next_payment_col
  df_okra['Authorization_payLink'] = authorization_payLink_col
  df_okra['Authorization_type'] = authorization_type_col
  df_okra['Authorization_used'] = authorization_used_col
  df_okra['AuthorizationId'] = authorizationId_col
  df_okra['BankId'] = bankId_col
  df_okra['BankName'] = bankName_col
  df_okra['bankSlug'] = bankSlug_col
  df_okra['bankType'] = bankType_col
  df_okra['callbackURL'] = callbackURL_col
  df_okra['callback_code'] = callback_code_col
  df_okra['callback_type'] = callback_type_col 
  df_okra['callback_url'] = callback_url_col
  df_okra['code'] = code_col
  df_okra['country'] = country_col
  df_okra['current_project'] = current_project_col
  df_okra['customerEmail'] = customerEmail_col
  df_okra['customerId'] = customerId_col
  df_okra['ended_at'] = ended_at_col
  df_okra['env'] = env_col
  df_okra['extras'] = extras_col
  df_okra['identityType'] = identityType_col
  df_okra['login_type'] = login_type_col
  df_okra['message'] = message_col
  df_okra['meta'] = meta_col
  df_okra['method'] = method_col
  df_okra['options'] = options_col
  df_okra['owner'] = owner_col
  df_okra['record'] = record_col 
  df_okra['recordId'] = recordId_col
  df_okra['started_at'] = started_at_col
  df_okra['status_webhook'] = status_webhook_col
  df_okra['token'] = token_col
  df_okra['type'] = type_col
 
  return df_okra



def parse_LEADWAY_DF(df_leadway_success):

  '''
    la fonction permet de parser les types de requete "LEADWAY" sur tout le dataframe
  '''
    
  log_level_col = []
  api_request_col = []
  type_request_col = []
  phone_Col = []
  date_col = []
  endpoint_Col = []
  email_col = []
  message_sms_payload_col = []
  totalsent_col = []
  cost_col = []
  status_col = []
  account_number_col = []
  account_name_col = []
  bvn_col = []
  requestSuccessful_col = []
  responseMessage_col = []
  responseCode_col = []

  list_column_none_leadway_success = []
  list_column_none_leadway_success = [message_sms_payload_col, totalsent_col, cost_col, status_col, 
                                     account_number_col, bvn_col, requestSuccessful_col, responseMessage_col,
                                     responseCode_col, account_name_col, email_col, phone_Col, endpoint_Col]
  
  list_all_colum = []
  list_all_colum = [type_request_col, phone_Col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

  for index, row in df_leadway_success.iterrows():
    str_text = row['text']
    
    if not str_text.startswith('['):
      for i in range(len(list_all_colum)):
          list_all_colum[i].append(None)

    # check if the row contains "info" string
    if re.search('info', str_text):
        date_col.append(row['ts'])
        log_level = re.search('info', str_text)
        try:
          log_level_col.append(log_level.group(0))
        except AttributeError:
          log_level_col.append(None)             
        if 'mailto' not in str_text:
            if re.search('LEADWAY SUCCESS', str_text):
                  type_of_request = re.search('LEADWAY SUCCESS', str_text)
                  leadway_success_concat_text, index_first_succ, index_last_succ = parse_and_concatenate_Leadway_Success_Rows(df_leadway_success)
                  res_text_leadway = parse_Leadway_Success_Row(leadway_success_concat_text)
                  for o in range(len(list_column_none_leadway_success)):
                    list_column_none_leadway_success[o].append(None)

                  try:
                    type_request_col.append(type_of_request.group(0))
                  except AttributeError:
                    type_request_col.append(None)
                   
        elif re.search('OKRA PAYLOAD', str_text): # Nothing
          type_of_request = re.search('OKRA PAYLOAD', str_text)
        elif re.search('OKRA SUCCESS', str_text):   # Nothing
          type_of_request = re.search('OKRA SUCCESS', str_text)
        elif re.search('VTPASS SUCCESS', str_text):   # Nothing
          type_of_request = re.search('VTPASS SUCCESS', str_text)  

  df_leadway_success['Type_Request'] = type_request_col
  df_leadway_success['Phone_Number'] = phone_Col
  df_leadway_success['Date'] = date_col
  df_leadway_success['EndPoint'] = endpoint_Col
  df_leadway_success['Log_Level'] = log_level_col
  df_leadway_success['Email'] = email_col
  df_leadway_success['Message SMS Payload'] = message_sms_payload_col
  df_leadway_success['Total Sent'] = totalsent_col
  df_leadway_success['Cost'] = cost_col
  df_leadway_success['Status'] = status_col
  df_leadway_success['Account Number'] = account_number_col
  df_leadway_success['Account Name'] = account_name_col
  df_leadway_success['BVN'] = bvn_col
  df_leadway_success['Request Successful'] = requestSuccessful_col
  df_leadway_success['Response Message'] = responseMessage_col
  df_leadway_success['Response Code'] = responseCode_col
 
  return df_leadway_success



def parse_PROVIDUS_PAYLOAD_DF(df_providus_payload):
    """
    la fonction permet de parser les types de requete "PROVIDUS PAYLOAD" sur tout le dataframe
    """
    
    beneficiary_account_name_col = []
    beneficiary_account_number_col = []
    beneficiary_bank_col = []
    currency_code_col = []
    narration_col = []
    source_account_name_col = []
    transaction_amount_col = []
    transaction_reference_col = []
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []

    list_column_none_providus_payload = []
    list_column_none_providus_payload = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        beneficiary_account_number_col, 
                                        endpoint_col,
                                        account_number_col, account_name_col, bvn_col,
                                        requestSuccessful_col, responseMessage_col,
                                        responseCode_col]


    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col,
                    beneficiary_account_name_col, beneficiary_account_number_col, beneficiary_bank_col,
                    currency_code_col, narration_col, source_account_name_col, transaction_amount_col,
                    transaction_reference_col]

    for index, row in df_providus_payload.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            date_col.append(row['ts'])
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('PROVIDUS PAYLOAD', str_text):               
                    providus_payload = parse_wallet_sms_payload_success(str_text)
                    type_of_request = re.search('PROVIDUS PAYLOAD', str_text)
                       
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        beneficiary_account_name_col.append(providus_payload.get('beneficiaryAccountName'))
                    except AttributeError:
                        beneficiary_account_name_col.append(None)
                    try:
                        phone_col.append(providus_payload.get('beneficiaryAccountNumber'))
                    except AttributeError:
                        phone_col.append(None)
                    try:
                        beneficiary_bank_col.append(providus_payload.get('beneficiaryBank'))
                    except AttributeError:
                        beneficiary_bank_col.append(None)
                    try:
                        currency_code_col.append(providus_payload.get('currencyCode'))
                    except AttributeError:
                        currency_code_col.append(None)
                    try:
                        narration_col.append((providus_payload.get('narration')))
                    except AttributeError:
                        narration_col.append(None)
                    try:
                        source_account_name_col.append((providus_payload.get('sourceAccountName')))
                    except AttributeError:
                        source_account_name_col.append(None)
                    try:
                        transaction_amount_col.append((providus_payload.get('transactionAmount')))
                    except AttributeError:
                        transaction_amount_col.append(None)
                    try:
                        transaction_reference_col.append((providus_payload.get('transactionReference')))
                    except AttributeError:
                        transaction_reference_col.append(None)

                    for n in range(len(list_column_none_providus_payload)):
                        list_column_none_providus_payload[n].append(None) 
              
            elif re.search('OKRA PAYLOAD', str_text): # Nothing
                type_of_request = re.search('OKRA PAYLOAD', str_text)

            elif re.search('OKRA SUCCESS', str_text):   # Nothing
                type_of_request = re.search('OKRA SUCCESS', str_text)

            elif re.search('VTPASS SUCCESS', str_text):   # Nothing
                type_of_request = re.search('VTPASS SUCCESS', str_text)

   
    df_providus_payload['Beneficiary_Account_Name'] = beneficiary_account_name_col
    df_providus_payload['Beneficiary_Account_Number'] = beneficiary_account_number_col
    df_providus_payload['Beneficiary_Bank'] = beneficiary_bank_col
    df_providus_payload['Currency_Code'] = currency_code_col
    df_providus_payload['Narration'] = narration_col
    df_providus_payload['Source_Account_Name'] = source_account_name_col
    df_providus_payload['Transaction_Amount'] = transaction_amount_col
    df_providus_payload['Transaction_Reference'] = transaction_reference_col
    df_providus_payload['Type_Request'] = type_request_col
    df_providus_payload['Phone_Number'] = phone_col
    df_providus_payload['Date'] = date_col
    df_providus_payload['EndPoint'] = endpoint_col
    df_providus_payload['Log_Level'] = log_level_col
    df_providus_payload['Email'] = email_col
    df_providus_payload['Message SMS Payload'] = message_sms_payload_col
    df_providus_payload['Total Sent'] = totalsent_col
    df_providus_payload['Cost'] = cost_col
    df_providus_payload['Status'] = status_col
    df_providus_payload['Account Number'] = account_number_col
    df_providus_payload['Account Name'] = account_name_col
    df_providus_payload['BVN'] = bvn_col
    df_providus_payload['Request Successful'] = requestSuccessful_col
    df_providus_payload['Response Message'] = responseMessage_col
    df_providus_payload['Response Code'] = responseCode_col
 
    return df_providus_payload



def parse_PROVIDUS_SUCCESS_DF(df_providus_success):
    """
    la fonction permet de parser les types de requete "PROVIDUS SUCCESS" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    sessionId_col = []
    transaction_reference_col = []




    list_column_none_providus_success = []
    list_column_none_providus_success = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        phone_col, 
                                        endpoint_col,
                                        account_number_col, account_name_col, bvn_col,
                                        requestSuccessful_col]


    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col, 
                    sessionId_col, transaction_reference_col]

    for index, row in df_providus_success.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            date_col.append(row['ts'])
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('PROVIDUS SUCCESS', str_text):
                    providus_success = parse_wallet_sms_payload_success(str_text)
                    type_of_request = re.search('PROVIDUS SUCCESS', str_text)
                       
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        responseCode_col.append(providus_success.get('responseCode'))
                    except AttributeError:
                        responseCode_col.append(None)
                    try:
                        responseMessage_col.append(providus_success.get('responseMessage'))
                    except AttributeError:
                        responseMessage_col.append(None)
                    try:
                        sessionId_col.append(providus_success.get('sessionId'))
                    except AttributeError:
                        sessionId_col.append(None)
                    try:
                        transaction_reference_col.append(providus_success.get('transactionReference'))
                    except AttributeError:
                        transaction_reference_col.append(None)

                    for n in range(len(list_column_none_providus_success)):
                        list_column_none_providus_success[n].append(None) 

    
    df_providus_success['SessionId'] = sessionId_col
    df_providus_success['Transaction_Reference'] = transaction_reference_col
    df_providus_success['Type_Request'] = type_request_col
    df_providus_success['Phone_Number'] = phone_col
    df_providus_success['Date'] = date_col
    df_providus_success['EndPoint'] = endpoint_col
    df_providus_success['Log_Level'] = log_level_col
    df_providus_success['Email'] = email_col
    df_providus_success['Message SMS Payload'] = message_sms_payload_col
    df_providus_success['Total Sent'] = totalsent_col
    df_providus_success['Cost'] = cost_col
    df_providus_success['Status'] = status_col
    df_providus_success['Account Number'] = account_number_col
    df_providus_success['Account Name'] = account_name_col
    df_providus_success['BVN'] = bvn_col
    df_providus_success['Request Successful'] = requestSuccessful_col
    df_providus_success['Response Message'] = responseMessage_col
    df_providus_success['Response Code'] = responseCode_col
 
    return df_providus_success



def parse_VTPASS_PAYLOAD_DF(df_vtpass_payload):
    """
    la fonction permet de parser les types de requete "VTPASS PAYLOAD" sur tout le dataframe
    """

    amount_col = []
    request_id_col = []
    serviceID_col = []
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []


    list_column_none_providus_success = []
    list_column_none_providus_success = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        endpoint_col,
                                        account_number_col, account_name_col, bvn_col,
                                        requestSuccessful_col, responseMessage_col, responseCode_col]


    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col,
                    amount_col, request_id_col, serviceID_col]

    for index, row in df_vtpass_payload.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            date_col.append(row['ts'])
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('VTPASS PAYLOAD', str_text):
                    providus_success = parse_wallet_sms_payload_success(str_text)
                    type_of_request = re.search('VTPASS PAYLOAD', str_text)
                       
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        amount_col.append(providus_success.get('amount'))
                    except AttributeError:
                        amount_col.append(None)
                    try:
                        phone_col.append(providus_success.get('phone'))
                    except AttributeError:
                        phone_col.append(None)
                    try:
                        request_id_col.append(providus_success.get('request_id'))
                    except AttributeError:
                        request_id_col.append(None)
                    try:
                        serviceID_col.append(providus_success.get('serviceID'))
                    except AttributeError:
                        serviceID_col.append(None)

                    for n in range(len(list_column_none_providus_success)):
                        list_column_none_providus_success[n].append(None) 
              


    df_vtpass_payload['Service_ID'] = serviceID_col
    df_vtpass_payload['Amount'] = amount_col
    df_vtpass_payload['Request_Id'] = request_id_col
    df_vtpass_payload['Type_Request'] = type_request_col
    df_vtpass_payload['Phone_Number'] = phone_col
    df_vtpass_payload['Date'] = date_col
    df_vtpass_payload['EndPoint'] = endpoint_col
    df_vtpass_payload['Log_Level'] = log_level_col
    df_vtpass_payload['Email'] = email_col
    df_vtpass_payload['Message SMS Payload'] = message_sms_payload_col
    df_vtpass_payload['Total Sent'] = totalsent_col
    df_vtpass_payload['Cost'] = cost_col
    df_vtpass_payload['Status'] = status_col
    df_vtpass_payload['Account Number'] = account_number_col
    df_vtpass_payload['Account Name'] = account_name_col
    df_vtpass_payload['BVN'] = bvn_col
    df_vtpass_payload['Request Successful'] = requestSuccessful_col
    df_vtpass_payload['Response Message'] = responseMessage_col
    df_vtpass_payload['Response Code'] = responseCode_col
 
    return df_vtpass_payload



def parse_LEADWAY_ERROR_DF(df_leadway_error):
    """
    la fonction permet de parser les types de requete "LEADWAY ERROR" sur tout le dataframe
    """
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    error_code_col = []
    error_number_col = []
    port_col = []
    syscall_col = []
    address_col = []
  
    #list_column_none_level_log_error = []
    # columns set to None
    list_column_none = [message_sms_payload_col, totalsent_col, cost_col, status_col, email_col,
                        endpoint_col, bvn_col, requestSuccessful_col, responseMessage_col,
                        responseCode_col, account_name_col, account_number_col, phone_col]
    

    # columns which will be populated
    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col,
                    error_code_col, error_number_col, port_col, syscall_col, 
                    address_col]

    for index, row in df_leadway_error.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        if re.search('error', str_text):
            log_level = re.search('error', str_text)
            date_col.append(row['ts'])
                
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)

            if re.search('LEADWAY ERROR', str_text):
                type_of_request = re.search('LEADWAY ERROR', str_text)
                leadway_error = parse_wallet_sms_payload_success(str_text)
                try:
                    error_code_col.append(leadway_error.get('code'))
                except AttributeError:
                    error_code_col.append(None)
                try:
                    error_number_col.append(leadway_error.get('errno'))
                except AttributeError:
                    error_number_col.append(None)
                try:
                    address_col.append(leadway_error.get('address'))
                except AttributeError:
                    address_col.append(None)
                try:
                    port_col.append(leadway_error.get('port'))
                except AttributeError:
                    port_col.append(None)
                try:
                    syscall_col.append(leadway_error.get('syscall'))
                except AttributeError:
                    syscall_col.append(None)      
                try:
                    type_request_col.append(type_of_request.group(0))
                except AttributeError:
                    type_request_col.append(None)

                for p in range(len(list_column_none)):
                    list_column_none[p].append(None)

    # set columns to their corresponding list values

    df_leadway_error['Type_Request'] = type_request_col
    df_leadway_error['Phone_Number'] =phone_col
    df_leadway_error['Date'] = date_col
    df_leadway_error['EndPoint'] = endpoint_col
    df_leadway_error['Log_Level'] = log_level_col
    df_leadway_error['Email'] = email_col
    df_leadway_error['Message SMS Payload'] = message_sms_payload_col
    df_leadway_error['Total Sent'] = totalsent_col
    df_leadway_error['Cost'] = cost_col
    df_leadway_error['Status'] = status_col
    df_leadway_error['Account Number'] = account_number_col
    df_leadway_error['Account Name'] = account_name_col
    df_leadway_error['BVN'] = bvn_col
    df_leadway_error['Request Successful'] = requestSuccessful_col
    df_leadway_error['Response Message'] = responseMessage_col
    df_leadway_error['Response Code'] = responseCode_col
    df_leadway_error['Error Code'] = error_code_col
    df_leadway_error['Error Number'] = error_number_col
    df_leadway_error['Error Port'] = port_col
    df_leadway_error['Error Syscall'] = syscall_col
    df_leadway_error['Error Address'] = address_col


    return df_leadway_error




def parse_PROVIDUS_TRANSFER_ERROR_DF(df_providus_transfer_error):
    """
    la fonction permet de parser les types de requete "PROVIDUS TRANSFER ERROR" sur tout le dataframe
    """
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    error_code_col = []
    error_number_col = []
    port_col = []
    syscall_col = []
    address_col = []
  
    #list_column_none_level_log_error = []
    # columns set to None
    list_column_none = [message_sms_payload_col, totalsent_col, cost_col, status_col, email_col,
                        endpoint_col, bvn_col, requestSuccessful_col, responseMessage_col,
                        responseCode_col, account_name_col, account_number_col, phone_col]
    

    # columns which will be populated
    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col,
                    error_code_col, error_number_col, port_col, syscall_col, 
                    address_col]  

    for index, row in df_providus_transfer_error.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        if re.search('error', str_text):
            log_level = re.search('error', str_text)
            date_col.append(row['ts'])
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)

            if re.search('PROVIDUS TRANSFER ERROR', str_text):
                type_of_request = re.search('PROVIDUS TRANSFER ERROR', str_text)
                leadway_error = parse_providus_transfer_error_function(str_text)
                try:
                    error_code_col.append(leadway_error.get('code'))
                except AttributeError:
                    error_code_col.append(None)
                try:
                    error_number_col.append(leadway_error.get('errno'))
                except AttributeError:
                    error_number_col.append(None)
                try:
                    address_col.append(leadway_error.get('address'))
                except AttributeError:
                    address_col.append(None)
                try:
                    port_col.append(leadway_error.get('port'))
                except AttributeError:
                    port_col.append(None)
                try:
                    syscall_col.append(leadway_error.get('syscall'))
                except AttributeError:
                    syscall_col.append(None)      
                try:
                    type_request_col.append(type_of_request.group(0))
                except AttributeError:
                    type_request_col.append(None)

                for p in range(len(list_column_none)):
                    list_column_none[p].append(None)

    # set columns to their corresponding list values

    df_providus_transfer_error['Type_Request'] = type_request_col
    df_providus_transfer_error['Phone_Number'] =phone_col
    df_providus_transfer_error['Date'] = date_col
    df_providus_transfer_error['EndPoint'] = endpoint_col
    df_providus_transfer_error['Log_Level'] = log_level_col
    df_providus_transfer_error['Email'] = email_col
    df_providus_transfer_error['Message SMS Payload'] = message_sms_payload_col
    df_providus_transfer_error['Total Sent'] = totalsent_col
    df_providus_transfer_error['Cost'] = cost_col
    df_providus_transfer_error['Status'] = status_col
    df_providus_transfer_error['Account Number'] = account_number_col
    df_providus_transfer_error['Account Name'] = account_name_col
    df_providus_transfer_error['BVN'] = bvn_col
    df_providus_transfer_error['Request Successful'] = requestSuccessful_col
    df_providus_transfer_error['Response Message'] = responseMessage_col
    df_providus_transfer_error['Response Code'] = responseCode_col
    df_providus_transfer_error['Error Code'] = error_code_col
    df_providus_transfer_error['Error Number'] = error_number_col
    df_providus_transfer_error['Error Port'] = port_col
    df_providus_transfer_error['Error Syscall'] = syscall_col
    df_providus_transfer_error['Error Address'] = address_col


    return df_providus_transfer_error



def parse_PROVIDUS_TRANSFER_SUCCESS_DF(df_providus_transfer_success):
    """
    la fonction permet de parser les types de requete "PROVIDUS TRANSFER SUCCESS" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    sessionId_col = []
    transaction_reference_col = []
    message_error_col = []


    list_column_none_providus_success = []
    list_column_none_providus_success = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        phone_col, 
                                        endpoint_col,
                                        account_number_col, account_name_col, bvn_col,
                                        requestSuccessful_col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col, 
                    sessionId_col, transaction_reference_col]

    for index, row in df_providus_transfer_success.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            date_col.append(row['ts'])
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None) 

            if re.search('PROVIDUS TRANSFER SUCCESS', str_text):
              
              if 'Faithfully yours, nginx' in str_text:
                  error_text = 'Sorry, the page you are looking for is currently unavailable, Please try again later.'
                  message_error_col.append(error_text)
                  responseCode_col.append(None)
                  responseMessage_col.append(None)
                  sessionId_col.append(None)
                  transaction_reference_col.append(None)
                  type_request_col.append(type_of_request.group(0))
              else:
                  message_error_col.append(None)
                  try:
                    providus_transfer_success = parse_wallet_sms_payload_success(str_text)
                  except json.decoder.JSONDecodeError:
                    #print(str_text)
                    raise

                  type_of_request = re.search('PROVIDUS TRANSFER SUCCESS', str_text)
                      
                  try:
                      type_request_col.append(type_of_request.group(0))
                  except AttributeError:
                      type_request_col.append(None)
                  try:
                      responseCode_col.append(providus_transfer_success.get('responseCode'))
                  except AttributeError:
                      responseCode_col.append(None)
                  try:
                      responseMessage_col.append(providus_transfer_success.get('responseMessage'))
                  except AttributeError:
                      responseMessage_col.append(None)
                  try:
                      sessionId_col.append(providus_transfer_success.get('sessionId'))
                  except AttributeError:
                      sessionId_col.append(None)
                  try:
                      transaction_reference_col.append(providus_transfer_success.get('transactionReference'))
                  except AttributeError:
                      transaction_reference_col.append(None)
              for n in range(len(list_column_none_providus_success)):
                  list_column_none_providus_success[n].append(None) 
    
    df_providus_transfer_success['SessionId'] = sessionId_col
    df_providus_transfer_success['Transaction_Reference'] = transaction_reference_col
    df_providus_transfer_success['Type_Request'] = type_request_col
    df_providus_transfer_success['Phone_Number'] = phone_col
    df_providus_transfer_success['Date'] = date_col
    df_providus_transfer_success['EndPoint'] = endpoint_col
    df_providus_transfer_success['Log_Level'] = log_level_col
    df_providus_transfer_success['Email'] = email_col
    df_providus_transfer_success['Message SMS Payload'] = message_sms_payload_col
    df_providus_transfer_success['Total Sent'] = totalsent_col
    df_providus_transfer_success['Cost'] = cost_col
    df_providus_transfer_success['Status'] = status_col
    df_providus_transfer_success['Account Number'] = account_number_col
    df_providus_transfer_success['Account Name'] = account_name_col
    df_providus_transfer_success['BVN'] = bvn_col
    df_providus_transfer_success['Request Successful'] = requestSuccessful_col
    df_providus_transfer_success['Response Message'] = responseMessage_col
    df_providus_transfer_success['Response Code'] = responseCode_col
    df_providus_transfer_success['Error Message Providus Transfer'] = message_error_col
 
    return df_providus_transfer_success




def parse_PROVIDUS_SETTLEMENT_INFO_DF(df_providus_settlement_info):
    """
    la fonction permet de parser les types de requete "PROVIDUS SETTLEMENT INFO" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    sessionId_col = []
    channelId_col = []
    feeAmount_col = []
    currency_col = []
    initiation_tranRef_col = []
    settled_amount_col = []
    settlementId_col = []
    source_account_name_col = []
    source_account_number_col = []
    source_bank_name_col = []
    tran_date_time_col = []
    tran_remarks_col = []
    transaction_amount_col = []
    vat_amount_col = []


    list_column_none_providus_success = []
    list_column_none_providus_success = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        account_number_col, 
                                        endpoint_col,
                                        account_name_col, bvn_col,
                                        requestSuccessful_col, responseCode_col, responseMessage_col]


    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col, 
                    sessionId_col, channelId_col, feeAmount_col, currency_col, initiation_tranRef_col,
                    settled_amount_col, settlementId_col, source_account_name_col, source_account_number_col,
                    source_bank_name_col, tran_date_time_col, tran_remarks_col, transaction_amount_col, vat_amount_col]

    for index, row in df_providus_settlement_info.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            date_col.append(row['ts'])
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('PROVIDUS SETTLEMENT INFO', str_text):
                    providus_settlement_info = parse_wallet_sms_payload_success(str_text)
                    #print(providus_settlement_info)
                    type_of_request = re.search('PROVIDUS SETTLEMENT INFO', str_text)
          
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        phone_col.append(providus_settlement_info.get('accountNumber'))
                    except AttributeError:
                        phone_col.append(None)
                    try:
                        channelId_col.append(providus_settlement_info.get('channelId'))
                    except AttributeError:
                        channelId_col.append(None)
                    try:
                        sessionId_col.append(providus_settlement_info.get('sessionId'))
                    except AttributeError:
                        sessionId_col.append(None)
                    try:
                        currency_col.append(providus_settlement_info.get('currency'))
                    except AttributeError:
                        currency_col.append(None)
                    try:
                        feeAmount_col.append(providus_settlement_info.get('feeAmount'))
                    except AttributeError:
                        feeAmount_col.append(None)
                    try:
                        initiation_tranRef_col.append(providus_settlement_info.get('initiationTranRef'))
                    except AttributeError:
                        initiation_tranRef_col.append(None)
                    try:
                        settled_amount_col.append(providus_settlement_info.get('settledAmount'))
                    except AttributeError:
                        settled_amount_col.append(None)
                    try:
                        settlementId_col.append(providus_settlement_info.get('settlementId'))
                    except AttributeError:
                        settlementId_col.append(None)
                    try:
                        source_account_name_col.append(providus_settlement_info.get('sourceAccountName'))
                    except AttributeError:
                        source_account_name_col.append(None)
                    try:
                        source_account_number_col.append(providus_settlement_info.get('sourceAccountNumber'))
                    except AttributeError:
                        source_account_number_col.append(None)
                    try:
                        source_bank_name_col.append(providus_settlement_info.get('sourceBankName'))
                    except AttributeError:
                        source_bank_name_col.append(None)
                    try:
                        tran_date_time_col.append(providus_settlement_info.get('tranDateTime'))
                    except AttributeError:
                        tran_date_time_col.append(None)
                    try:
                        tran_remarks_col.append(providus_settlement_info.get('tranRemarks'))
                    except AttributeError:
                        tran_remarks_col.append(None)
                    try:
                        transaction_amount_col.append(providus_settlement_info.get('transactionAmount'))
                    except AttributeError:
                        transaction_amount_col.append(None)
                    try:
                        vat_amount_col.append(providus_settlement_info.get('vatAmount'))
                    except AttributeError:
                        vat_amount_col.append(None)

                    for n in range(len(list_column_none_providus_success)):
                        list_column_none_providus_success[n].append(None) 

    
    df_providus_settlement_info['Vat_Amount'] = vat_amount_col
    df_providus_settlement_info['Transaction_Amount'] = transaction_amount_col
    df_providus_settlement_info['Tran_Remarks'] = tran_remarks_col
    df_providus_settlement_info['Tran_Date_Time'] = tran_date_time_col
    df_providus_settlement_info['Source_Bank_Name'] = source_bank_name_col
    df_providus_settlement_info['Source_Account_Number'] = source_account_number_col
    df_providus_settlement_info['Source_Account_Name'] = source_account_name_col
    df_providus_settlement_info['SettlementId'] = settlementId_col
    df_providus_settlement_info['settled_Amount'] = settled_amount_col
    df_providus_settlement_info['Initiation_TranRef'] = initiation_tranRef_col
    df_providus_settlement_info['FeeAmount'] = feeAmount_col
    df_providus_settlement_info['Currency'] = currency_col
    df_providus_settlement_info['ChannelId'] = channelId_col
    df_providus_settlement_info['SessionId'] = sessionId_col
    df_providus_settlement_info['Type_Request'] = type_request_col
    df_providus_settlement_info['Phone_Number'] = phone_col
    df_providus_settlement_info['Date'] = date_col
    df_providus_settlement_info['EndPoint'] = endpoint_col
    df_providus_settlement_info['Log_Level'] = log_level_col
    df_providus_settlement_info['Email'] = email_col
    df_providus_settlement_info['Message SMS Payload'] = message_sms_payload_col
    df_providus_settlement_info['Total Sent'] = totalsent_col
    df_providus_settlement_info['Cost'] = cost_col
    df_providus_settlement_info['Status'] = status_col
    df_providus_settlement_info['Account Number'] = account_number_col
    df_providus_settlement_info['Account Name'] = account_name_col
    df_providus_settlement_info['BVN'] = bvn_col
    df_providus_settlement_info['Request Successful'] = requestSuccessful_col
    df_providus_settlement_info['Response Message'] = responseMessage_col
    df_providus_settlement_info['Response Code'] = responseCode_col
 
    return df_providus_settlement_info




def parse_PROVIDUS_VERIFY_SETTLEMENT_INFO_DF(df_providus_verify_settlement_info):
    """
    la fonction permet de parser les types de requete "PROVIDUS VERIFY SETTLEMENT INFO" sur tout le dataframe
    """

    log_level_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    sessionId_col = []
    channelId_col = []
    feeAmount_col = []
    currency_col = []
    initiation_tranRef_col = []
    settled_amount_col = []
    settlementId_col = []
    source_account_name_col = []
    source_account_number_col = []
    source_bank_name_col = []
    tran_date_time_col = []
    tran_remarks_col = []
    transaction_amount_col = []
    vat_amount_col = []


    list_column_none_providus_success = []
    list_column_none_providus_success = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        account_number_col, 
                                        endpoint_col, 
                                        account_name_col, bvn_col,
                                        requestSuccessful_col, responseMessage_col, responseCode_col]


    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col, 
                    sessionId_col, channelId_col, feeAmount_col, currency_col, initiation_tranRef_col,
                    settled_amount_col, settlementId_col, source_account_name_col, source_account_number_col,
                    source_bank_name_col, tran_date_time_col, tran_remarks_col, transaction_amount_col, vat_amount_col]

    for index, row in df_providus_verify_settlement_info.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            date_col.append(row['ts'])
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('PROVIDUS VERIFY SETTLEMENT INFO', str_text):
                    providus_verify_settlement_info = parse_wallet_sms_payload_success(str_text)
                    #print(providus_verify_settlement_info)
                    type_of_request = re.search('PROVIDUS VERIFY SETTLEMENT INFO', str_text)


                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        phone_col.append(providus_verify_settlement_info.get('accountNumber'))
                    except AttributeError:
                        phone_col.append(None)
                    try:
                        channelId_col.append(providus_verify_settlement_info.get('channelId'))
                    except AttributeError:
                        channelId_col.append(None)
                    try:
                        sessionId_col.append(providus_verify_settlement_info.get('sessionId'))
                    except AttributeError:
                        sessionId_col.append(None)
                    try:
                        currency_col.append(providus_verify_settlement_info.get('currency'))
                    except AttributeError:
                        currency_col.append(None)
                    try:
                        feeAmount_col.append(providus_verify_settlement_info.get('feeAmount'))
                    except AttributeError:
                        feeAmount_col.append(None)
                    try:
                        initiation_tranRef_col.append(providus_verify_settlement_info.get('initiationTranRef'))
                    except AttributeError:
                        initiation_tranRef_col.append(None)
                    try:
                        settled_amount_col.append(providus_verify_settlement_info.get('settledAmount'))
                    except AttributeError:
                        settled_amount_col.append(None)
                    try:
                        settlementId_col.append(providus_verify_settlement_info.get('settlementId'))
                    except AttributeError:
                        settlementId_col.append(None)
                    try:
                        source_account_name_col.append(providus_verify_settlement_info.get('sourceAccountName'))
                    except AttributeError:
                        source_account_name_col.append(None)
                    try:
                        source_account_number_col.append(providus_verify_settlement_info.get('sourceAccountNumber'))
                    except AttributeError:
                        source_account_number_col.append(None)
                    try:
                        source_bank_name_col.append(providus_verify_settlement_info.get('sourceBankName'))
                    except AttributeError:
                        source_bank_name_col.append(None)
                    try:
                        tran_date_time_col.append(providus_verify_settlement_info.get('tranDateTime'))
                    except AttributeError:
                        tran_date_time_col.append(None)
                    try:
                        tran_remarks_col.append(providus_verify_settlement_info.get('tranRemarks'))
                    except AttributeError:
                        tran_remarks_col.append(None)
                    try:
                        transaction_amount_col.append(providus_verify_settlement_info.get('transactionAmount'))
                    except AttributeError:
                        transaction_amount_col.append(None)
                    try:
                        vat_amount_col.append(providus_verify_settlement_info.get('vatAmount'))
                    except AttributeError:
                        vat_amount_col.append(None)

                    for n in range(len(list_column_none_providus_success)):
                        list_column_none_providus_success[n].append(None)  
    
    df_providus_verify_settlement_info['Vat_Amount'] = vat_amount_col
    df_providus_verify_settlement_info['Transaction_Amount'] = transaction_amount_col
    df_providus_verify_settlement_info['Tran_Remarks'] = tran_remarks_col
    df_providus_verify_settlement_info['Tran_Date_Time'] = tran_date_time_col
    df_providus_verify_settlement_info['Source_Bank_Name'] = source_bank_name_col
    df_providus_verify_settlement_info['Source_Account_Number'] = source_account_number_col
    df_providus_verify_settlement_info['Source_Account_Name'] = source_account_name_col
    df_providus_verify_settlement_info['SettlementId'] = settlementId_col
    df_providus_verify_settlement_info['settled_Amount'] = settled_amount_col
    df_providus_verify_settlement_info['Initiation_TranRef'] = initiation_tranRef_col
    df_providus_verify_settlement_info['FeeAmount'] = feeAmount_col
    df_providus_verify_settlement_info['Currency'] = currency_col
    df_providus_verify_settlement_info['ChannelId'] = channelId_col
    df_providus_verify_settlement_info['SessionId'] = sessionId_col
    df_providus_verify_settlement_info['Type_Request'] = type_request_col
    df_providus_verify_settlement_info['Phone_Number'] = phone_col
    df_providus_verify_settlement_info['Date'] = date_col
    df_providus_verify_settlement_info['EndPoint'] = endpoint_col
    df_providus_verify_settlement_info['Log_Level'] = log_level_col
    df_providus_verify_settlement_info['Email'] = email_col
    df_providus_verify_settlement_info['Message SMS Payload'] = message_sms_payload_col
    df_providus_verify_settlement_info['Total Sent'] = totalsent_col
    df_providus_verify_settlement_info['Cost'] = cost_col
    df_providus_verify_settlement_info['Status'] = status_col
    df_providus_verify_settlement_info['Account Number'] = account_number_col
    df_providus_verify_settlement_info['Account Name'] = account_name_col
    df_providus_verify_settlement_info['BVN'] = bvn_col
    df_providus_verify_settlement_info['Request Successful'] = requestSuccessful_col
    df_providus_verify_settlement_info['Response Message'] = responseMessage_col
    df_providus_verify_settlement_info['Response Code'] = responseCode_col
 
    return df_providus_verify_settlement_info



def subfilter_DF(df_to_process):
    
    in_df_providus_verify_settlement_info = df_to_process[df_to_process['text'].str.contains('PROVIDUS VERIFY SETTLEMENT INFO')]
    in_df_providus_settlement_info = df_to_process[df_to_process['text'].str.contains('PROVIDUS SETTLEMENT INFO')]
    in_df_providus_transfer_success = df_to_process[df_to_process['text'].str.contains('PROVIDUS TRANSFER SUCCESS')]
    in_df_providus_transfer_payload = df_to_process[df_to_process['text'].str.contains('PROVIDUS TRANSFER PAYLOAD')]
    in_df_sms_reponse = df_to_process[df_to_process['text'].str.contains('SMS REPONSE')]
    in_df_providus_transfer_error = df_to_process[df_to_process['text'].str.contains('PROVIDUS TRANSFER ERROR')]
    in_df_providus_success = df_to_process[df_to_process['text'].str.contains('PROVIDUS SUCCESS')]
    in_df_providus_payload = df_to_process[df_to_process['text'].str.contains('PROVIDUS PAYLOAD')]
    in_df_vtpass_payload = df_to_process[df_to_process['text'].str.contains('VTPASS PAYLOAD')]
    in_df_leadway_error = df_to_process[df_to_process['text'].str.contains('LEADWAY ERROR')]
    in_df_error = df_to_process[df_to_process['text'].str.contains('LOAN ERROR')]
    in_df_api_request = df_to_process[df_to_process['text'].str.contains('API REQUEST')]
    in_df_client_mobile_login = df_to_process[df_to_process['text'].str.contains('CLIENT MOBILE LOGIN')]
    in_df_sms_payload = df_to_process[df_to_process['text'].str.contains('SMS PAYLOAD')]
    in_df_sms_success = df_to_process[df_to_process['text'].str.contains('SMS SUCCESS')]
    in_df_wallet_success = df_to_process[df_to_process['text'].str.contains('WALLET SUCCESS')]
    in_df_okra_webhook = df_to_process[df_to_process['text'].str.contains('OKRA WEBHOOK')]
    in_df_leadway = df_to_process[df_to_process['text'].str.contains('LEADWAY')]

    return  in_df_providus_verify_settlement_info, \
            in_df_providus_settlement_info, \
            in_df_providus_transfer_success, \
            in_df_providus_transfer_payload, \
            in_df_sms_reponse, \
            in_df_providus_transfer_error, \
            in_df_providus_success, \
            in_df_providus_payload, \
            in_df_vtpass_payload, \
            in_df_leadway_error, \
            in_df_error, \
            in_df_api_request, \
            in_df_client_mobile_login, \
            in_df_sms_payload,\
            in_df_sms_success,\
            in_df_wallet_success,\
            in_df_okra_webhook,\
            in_df_leadway,\
            
            


def concatenate_info_DF(df_to_process, out_path = "./liberta_leasing"):
    """
    function which creates a clean final  dataframe with all parsed transaction
    """

    
    input_df_error ,input_df_api_request,input_df_client_mobile_login,input_df_sms_payload,\
        input_df_sms_success,input_df_wallet_success,input_df_okra_webhook, input_df_leadway, input_df_providus_payload,\
        input_df_providus_success, input_df_vtpass_payload, input_df_leadway_error, input_df_providus_transfer_error,\
        input_df_providus_transfer_success, input_df_providus_settlement_info, input_df_providus_verify_settlement_info = subfilter_DF(df_to_process)

    resultat_df_error = parse_ERROR_DF(input_df_error)
    resutat_df_api =  parse_API_REQUEST_DF(input_df_api_request)
    resutat_df_client_mobile_login = parse_CLIENT_MOBILE_LOGIN_DF(input_df_client_mobile_login)
    resutat_df_sms_payload = parse_SMS_PAYLOAD_DF(input_df_sms_payload)
    resutat_df_sms_success = parse_SMS_SUCCESS_DF(input_df_sms_success)
    resutat_df_wallet_success = parse_WALLET_SUCCESS_DF(input_df_wallet_success)
    df_okra_webhook = parse_OKRA_WEBHOOK_DF(input_df_okra_webhook)
    resutat_df_leadway = parse_LEADWAY_DF(input_df_leadway)
    resultat_df_providus_payload = parse_PROVIDUS_PAYLOAD_DF(input_df_providus_payload)
    resultat_df_providus_success = parse_PROVIDUS_SUCCESS_DF(input_df_providus_success)
    resultat_df_vtpass_payload = parse_VTPASS_PAYLOAD_DF(input_df_vtpass_payload)
    resultat_df_leadway_error = parse_LEADWAY_ERROR_DF(input_df_leadway_error)
    resultat_df_providus_transfer_error = parse_PROVIDUS_TRANSFER_ERROR_DF(input_df_providus_transfer_error)
    resultat_df_providus_transfer_success = parse_PROVIDUS_TRANSFER_SUCCESS_DF(input_df_providus_transfer_success)
    resultat_df_providus_settlement_info = parse_PROVIDUS_SETTLEMENT_INFO_DF(input_df_providus_settlement_info)
    resultat_df_providus_verify_settlement_info = parse_PROVIDUS_VERIFY_SETTLEMENT_INFO_DF(input_df_providus_verify_settlement_info)

    # List of our processed dataframes
    pdList = [resultat_df_error, 
            resutat_df_api, 
            resutat_df_client_mobile_login, 
            resutat_df_sms_payload, 
            resutat_df_sms_success, 
            resutat_df_wallet_success, 
            df_okra_webhook,
            resutat_df_leadway,
            resultat_df_providus_payload,
            resultat_df_providus_success,
            resultat_df_vtpass_payload,
            resultat_df_leadway_error,
            resultat_df_providus_transfer_error,
            resultat_df_providus_transfer_success,
            resultat_df_providus_settlement_info,
            resultat_df_providus_verify_settlement_info]  

    # concatenate the dataframes on the axis=1
    df_final = pd.concat(pdList)
    df_final.to_csv(os.path.join(out_path,'nirra_log_bot.csv'), index=None, sep= ',')

    return df_final
    
    
def get_unique_numbers_DF(final_df):
    """
    function which returns all users phone number ( =identification)
    """
    assert ('Phone_Number' in final_df.columns) == True
    all_users_phone_number = [element for element in final_df['Phone_Number'].unique() if element != None]
    return all_users_phone_number
    


def save_info_per_phone_number_DF(final_df):
    """
    function which saves all dataframes by phone number ( =identification)
    """
    os.makedirs("./liberta_leasing/files", exist_ok=True)
    all_users_phone_num = get_unique_numbers_DF(final_df)
    for phoneNumber in tqdm(all_users_phone_num):
        df_phone_num = final_df[final_df['Phone_Number'] == phoneNumber]
        df_phone_num.to_csv(f'./liberta_leasing/files/{phoneNumber}.csv', index=None)



if __name__=='__main__':

    """     text_type_request = \
    "[info] - [""[WALLET SUCCESS]:"",""{\""account_number\"":\""9980432021\"",\""account_name\"":\""LIBERTA(Kayode  Ajao)\"",\""bvn\"":\""22213975706\"",\""requestSuccessful\"":true,\""responseMessage\"":\""Reserved Account Generated Successfully\"",\""responseCode\"":\""00\""}""]"
    """ 
    #parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(text_type_request)



    # step 1: convert json to csv
    process_json(path_file_json="./nirra-log-bot", 
                dest_path="./liberta_leasing")
    
    # step 2: extract info from all csv
    all_csv = glob2.glob("./liberta_leasing/nirra-log-bot/*.csv")

    #csv_file_to_process = "./liberta_leasing/2021-03-27.csv"
    csv_file_to_process = "./liberta_leasing"

    #df_to_process = pd.read_csv(csv_file_to_process, sep='|', error_bad_lines=False)
    csv_files = glob.glob(os.path.join("./liberta_leasing/nirra-log-bot", "*.csv"))
    csv_files = sorted(csv_files)
    all_df_list = [] # list of all dataframe
    for f in tqdm.tqdm(csv_files):    
    # read the csv file
    #col_names = ["type", "subtype", "text", "ts", "bot_id"]
      df=pd.read_csv(f, sep="|")
      all_df_list.append(df) 
    
    df_raw = pd.concat(all_df_list, ignore_index=True)
    df_raw.to_csv('./liberta_leasing/nirra_log_bot.csv', index=None, sep='|')
    #finalized_df = concatenate_info_DF(csv_files, out_path = "./liberta_leasing")


    ### Dataframe LOAN PAYLOAD

    df_raw['text'].fillna('', inplace=True)
    loan_payload = df_raw[df_raw['text'].str.contains('LOAN PAYLOAD')]
    df_loan_payload = parse_row_LOAN_PAYLOAD(loan_payload)

    ### Dataframe API REQUEST

    df_raw['text'].fillna('', inplace=True)
    api_request = df_raw[df_raw['text'].str.contains('API REQUEST')]
    df_api_request = parse_API_REQUEST_DF(api_request)

    ### Dataframe CLIENT LOGIN MOBILE

    df_raw['text'].fillna('', inplace=True)
    client_mobile_login = df_raw[df_raw['text'].str.contains('CLIENT MOBILE LOGIN')]
    df_client_mobile_login = parse_CLIENT_MOBILE_LOGIN_DF(client_mobile_login)

    ### Dataframe SMS PAYLOAD

    df_raw['text'].fillna('', inplace=True)
    sms_payload = df_raw[df_raw['text'].str.contains('SMS PAYLOAD')]
    df_sms_payload = parse_SMS_PAYLOAD_DF(sms_payload)

    ### Dataframe SMS SUCCESS

    df_raw['text'].fillna('', inplace=True)
    sms_success = df_raw[df_raw['text'].str.contains('SMS SUCCESS')]
    df_sms_success = parse_SMS_SUCCESS_DF(sms_success)

    ### Dataframe WALLET SUCCESS

    df_raw['text'].fillna('', inplace=True)
    wallet_success = df_raw[df_raw['text'].str.contains('WALLET SUCCESS')]
    df_wallet_success = parse_WALLET_SUCCESS_DF(wallet_success)

    ### Dataframe OKRA WEBHOOK

    df_raw['text'].fillna('', inplace=True)
    df_okra_webhook = parse_OKRA_WEBHOOK_DF(df_raw[df_raw['text'].str.contains('OKRA WEBHOOK')])

    ### Dataframe PROVIDUS PAYLOAD

    df_raw['text'].fillna('', inplace=True)
    providus_payload_df = df_raw[df_raw['text'].str.contains('PROVIDUS PAYLOAD')]
    df_providus_payload = parse_PROVIDUS_PAYLOAD_DF(providus_payload_df)

    ### Dataframe PROVIDUS SUCCESS

    providus_success_df = df_raw[df_raw['text'].str.contains('PROVIDUS SUCCESS')]
    df_providus_success = parse_PROVIDUS_SUCCESS_DF(providus_success_df)

    ### Dataframe VTPASS PAYLOAD

    vtpass_payload = df_raw[df_raw['text'].str.contains('VTPASS PAYLOAD')]
    df_vtpass_payload = parse_VTPASS_PAYLOAD_DF(vtpass_payload)

    ### Dataframe LEADWAY ERROR

    lead_error = df_raw[df_raw['text'].str.contains('LEADWAY ERROR')]
    df_lead_error = parse_LEADWAY_ERROR_DF(lead_error)

    ### Dataframe PROVIDUS TRANSFER ERROR

    providus_transfer_error = df_raw[df_raw['text'].str.contains('PROVIDUS TRANSFER ERROR')]
    df_providus_transfer_error = parse_PROVIDUS_TRANSFER_ERROR_DF(providus_transfer_error)

    ### Dataframe PROVIDUS TRANSFER SUCCESS

    providus_transfer_success = df_raw[df_raw['text'].str.contains('PROVIDUS TRANSFER SUCCESS')]
    df_providus_transfer_success = parse_PROVIDUS_TRANSFER_SUCCESS_DF(providus_transfer_success)

    ### Dataframe PROVIDUS SETTLEMENT INFO

    providus_settlement_info = df_raw[df_raw['text'].str.contains('PROVIDUS SETTLEMENT INFO')]
    df_providus_settlement_info = parse_PROVIDUS_SETTLEMENT_INFO_DF(providus_settlement_info)

    ### Dataframe PROVIDUS VERIFY SETTLEMENT INFO

    providus_verify_settlement_info = df_raw[df_raw['text'].str.contains('PROVIDUS VERIFY SETTLEMENT INFO')]
    df_providus_verify_settlement_info = parse_PROVIDUS_VERIFY_SETTLEMENT_INFO_DF(providus_verify_settlement_info)

    ### Concatenation de tous les DF

    pdList = [df_api_request, df_client_mobile_login, df_loan_payload, df_lead_error, df_okra_webhook, df_providus_payload, df_providus_settlement_info,
              df_providus_success, df_providus_transfer_error, df_providus_transfer_success, df_providus_verify_settlement_info,
              df_sms_payload, df_sms_success, df_vtpass_payload, df_wallet_success]  # List of our dataframes
    df_final = pd.concat(pdList, axis=0)

    df_final.sort_values('Date')

    df_with_loan_amount = df_final[df_final['text'].str.contains('loan_amount')].copy()

    df_with_total_sent = df_final[df_final['text'].str.contains('totalsent')].copy()

    list_index_loan_amount = list(df_with_loan_amount.index)

    list_index_total_sent = list(df_with_total_sent.index)

    def difference_ts(date_in_timestamp):
      dt = datetime.datetime.fromtimestamp(date_in_timestamp)
      res = dt - datetime.timedelta(minutes=30) 
      return datetime.datetime.timestamp(res)


    for index in tqdm.tqdm(list_index_loan_amount):
      df_res = df_final[(df_final['ts'] <= df_final.loc[index, 'ts']) & (df_final['ts'] > difference_ts(df_final.loc[index, 'ts']))]
      list_index = df_res[df_res['Phone_Number'].isnull() == False].index.values
      if len(list_index) > 0:
        min_index = np.min(list_index)
        df_final['Phone_Number'][index] = df_res.loc[min_index, 'Phone_Number']


      ### Export DF

      #df_final.to_csv('/content/drive/MyDrive/datasets/final/nirra_log_bot_final.csv', index=None)

      all_users_phone_number = get_unique_numbers_DF(df_final)

      all_df_phone_number = [] # to get list of dataframes

      for phoneNumber in tqdm.tqdm(all_users_phone_number):
        df_phone_number = df_final[df_final['Phone_Number'] == phoneNumber] # récupérer les dataframes avec seulement les numéros de téléphone
        all_df_phone_number.append(df_phone_number)  # mettre chaque dataframe dans la

    ### Sauvegarder sur le disque les dataframes de chaque utilisateur

    # tqdm permet
    for phoneNumber in tqdm.tqdm(all_users_phone_number):
      df_phone_number = df_final[df_final['Phone_Number'] == phoneNumber]
      df_phone_number.to_csv(f'./files/{phoneNumber}.csv', index=None)

  ### se déplacer dans le dossier contenant les dataframes de chaque utilisateur

  #cd ('/content/drive/MyDrive/datasets/final/files')

      ### avoir le nombre de transactions de chaque utilisateur

      new_path = os.getcwd()
      list_df_with_diff_ts = get_all_df_diff_ts(new_path)


      another_path = os.getcwd()
      all_df_with_conversion_in_datetime = list_all_df_with_conversion_in_datetime(another_path)


      # on sauvegarde les fichiers en fonction du phone_number et du type_request parce qu'un meme utilisateur
      # peut effectuer plusieurs opérations de différents type request, en faisant ainsi, il n'y'a pas de risque d'écraser
      # certains fichiers du meme utilisateur.
      for ele in tqdm.tqdm(all_df_with_conversion_in_datetime):
        if ele.shape[0] > 0:
          phone_number = ele['Phone_Number'][0]
          type_request = ele['Type_Request'][0]
          t_r = type_request.replace(' ', '_')
          ele.to_csv(f'./new_files/{phone_number}_{t_r}.csv', index=None)

### Fonction pour calculer le nombre d'opérations par minute

      list_all_df_with_operations_per_minute = []
      for element in tqdm.tqdm(all_df_with_conversion_in_datetime):
        # pour debugger il faut utiliser try -  except et utiliser pass
        # ainsi, en cas de problème on pourrait savoir dans quel DataFrame il se trouve
        if element.shape[0] > 1:
          element.sort_values('TS_to_DateTime')
          list_all_df_with_operations_per_minute.append(df_number_of_operations_per_minute(element))

### Diviser la liste des dataframes avec operations par minute en DF de Train et de Test

# concatenation de la liste des dataframes
      df_with_operations_per_minute = pd.concat(list_all_df_with_operations_per_minute, axis=0).reset_index(drop=True)
          
      #Split dataset into train and test
      X_train_with_operations_per_minute, X_test_with_operations_per_minute = train_test_split(df_with_operations_per_minute, test_size = 0.3)

      ### DataSet de Train

      df_phone_number_with_operations_per_minute = X_train_with_operations_per_minute[['Phone_Number', 'Type_Request']]

      # on récupère tous les index du Dataframe de test
      X_train_with_operations_per_minute.index

      # on drop les colonnes Phone_Number et Type_Request
      X_train_with_operations_per_minute.drop(['Phone_Number', 'Type_Request'],  axis=1, inplace=True)

      # convertir le dataframe de X_train en une liste
      list_with_operations_per_minute = X_train_with_operations_per_minute.values.tolist()


      ### on applique le StandardScaler

       
      #Scaled_data = StandardScaler().fit_transform(list_resultats)
      Scaled_data_with_operations_per_minute = StandardScaler().fit_transform(list_with_operations_per_minute)

      ### Application de l'algorithme IsolationForest
      model = IsolationForest(contamination=0.005)
      model.fit(list_with_operations_per_minute)
      pickle.dump(model, open("model.pkl", "wb"))
