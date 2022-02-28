from genericpath import exists
import os
import glob
import glob2
import tqdm
import boto3
import json
import pandas as pd
import numpy as np
from pandas.plotting import scatter_matrix
from os import chdir as cd
import pickle
import datetime
from tqdm import tqdm
from sklearn.model_selection import train_test_split
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
from preproc import parse_ERROR_DF
from preproc import parse_row_LOAN_PAYLOAD
from preproc import parse_API_REQUEST_DF
from preproc import parse_CLIENT_MOBILE_LOGIN_DF
from preproc import parse_SMS_PAYLOAD_DF
from preproc import parse_SMS_SUCCESS_DF
from preproc import parse_WALLET_SUCCESS_DF
from preproc import parse_OKRA_WEBHOOK_DF
from preproc import parse_LEADWAY_DF
from preproc import parse_PROVIDUS_PAYLOAD_DF
from preproc import parse_PROVIDUS_SUCCESS_DF
from preproc import parse_VTPASS_PAYLOAD_DF
from preproc import parse_LEADWAY_ERROR_DF
from preproc import parse_PROVIDUS_TRANSFER_ERROR_DF
from preproc import parse_PROVIDUS_TRANSFER_SUCCESS_DF
from preproc import parse_PROVIDUS_SETTLEMENT_INFO_DF
from preproc import parse_PROVIDUS_VERIFY_SETTLEMENT_INFO_DF
from preproc import subfilter_DF
from preproc import concatenate_info_DF
from preproc import get_unique_numbers_DF
from preproc import save_info_per_phone_number_DF



def difference_ts(date_in_timestamp):
    dt = datetime.datetime.fromtimestamp(date_in_timestamp)
    res = dt - datetime.timedelta(minutes=30) 
    return datetime.datetime.timestamp(res)

### Dataframe LOAN ERROR

#df_raw['text'].fillna('', inplace=True)
#error = df_raw[df_raw['text'].str.contains('LOAN ERROR')]
#df_error = parse_ERROR_DF(error)

### Dataframe LOAN PAYLOAD 
#print("Processing Loan payload")

def preprocessing(df_raw, data_folder) : 

    access_key = 'AKIAWPXBUC7KKNTIR7UI'
    secret_access_key =  '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'
    bucket_name = 'dct-aws-cloud-labs-pa'

    client_s3 = boto3.client(
                's3',
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_access_key
              )

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

    for index in tqdm(list_index_loan_amount):
        df_res = df_final[(df_final['ts'] <= df_final.loc[index, 'ts']) & (df_final['ts'] > difference_ts(df_final.loc[index, 'ts']))]
        list_index = df_res[df_res['Phone_Number'].isnull() == False].index.values
        if len(list_index) > 0:
            min_index = np.min(list_index)
            df_final['Phone_Number'][index] = df_res.loc[min_index, 'Phone_Number']


    outname_1 = 'nirra_log_bot_final.csv'
    outdir_1 = f'{data_folder}/df_final/'
    #print('le chemin est: '+outdir_1)

    if not os.path.exists(outdir_1):
        os.makedirs(outdir_1, exist_ok=True)
    fullname_1 = os.path.join(outdir_1, outname_1) 
    print('chemin fullname_1: '+fullname_1)
    ### Export DF
    df_final.to_csv(fullname_1, index=None)

    # charger le fichier dans s3
    print('le chemin est: '+os.path.join(outdir_1, outname_1))
    client_s3.upload_file(os.path.join(outdir_1, outname_1), bucket_name, outname_1)

    all_users_phone_number = get_unique_numbers_DF(df_final)
    all_df_phone_number = [] # to get list of dataframes
    for phoneNumber in tqdm(all_users_phone_number):
        df_phone_number = df_final[df_final['Phone_Number'] == phoneNumber] # récupérer les dataframes avec seulement les numéros de téléphone
        all_df_phone_number.append(df_phone_number)  # mettre chaque dataframe dans la

    ### Sauvegarder sur le disque les dataframes de chaque utilisateur
    outdir_2 = f'{data_folder}/files_phone_number/'
    #upload_file_key_2 = outdir_2+ outname_1
    #client.upload_file(outname_1, upload_file_bucket, upload_file_key_1)
    if not os.path.exists(outdir_2):
        os.makedirs(outdir_2, exist_ok=True)
    print('le chemin 2 est : '+outdir_2)
    for phoneNumber in tqdm(all_users_phone_number):
        df_phone_number = df_final[df_final['Phone_Number'] == phoneNumber]
        df_phone_number.to_csv(f'{outdir_2}/{phoneNumber}.csv', index=None)
        #client_s3.upload_file(os.path.join(outdir_2, '{phoneNumber}.csv'), bucket_name, '{phoneNumber}.csv')
        # repertoire temp dont le nom doit changer selon qu'on fait l'entrainement ou la prédiction
        ### se déplacer dans le dossier contenant les dataframes de chaque utilisateur

    ### avoir le nombre de transactions de chaque utilisateur
    #new_path = os.getcwd()
    list_df_with_diff_ts = get_all_df_diff_ts(outdir_2)

    another_path = os.getcwd()
    all_df_with_conversion_in_datetime = list_all_df_with_conversion_in_datetime(outdir_2)
    # on sauvegarde les fichiers en fonction du phone_number et du type_request parce qu'un meme utilisateur
    # peut effectuer plusieurs opérations de différents type request, en faisant ainsi, il n'y'a pas de risque d'écraser
    # certains fichiers du meme utilisateur.
    outdir_3 = f'{data_folder}/files_phone_number_and_tr/'
    if not os.path.exists(outdir_3):
        os.makedirs(outdir_3, exist_ok=True)
    print('le chemin 3 est : '+outdir_3)

    for ele in tqdm(all_df_with_conversion_in_datetime):
        if ele.shape[0] > 0:
            phone_number = ele['Phone_Number'][0]
            type_request = ele['Type_Request'][0]
            t_r = type_request.replace(' ', '_')
            ele.to_csv(f'{outdir_3}/{phone_number}_{t_r}.csv', index=None)

    ### Fonction pour calculer le nombre d'opérations par minute
    list_all_df_with_operations_per_minute = []
    for element in tqdm(all_df_with_conversion_in_datetime):
    # pour debugger il faut utiliser try -  except et utiliser pass
    # ainsi, en cas de problème on pourrait savoir dans quel DataFrame il se trouve
        if element.shape[0] > 1:
            element.sort_values('TS_to_DateTime')
            list_all_df_with_operations_per_minute.append(df_number_of_operations_per_minute(element))

    print("** list_all_df_with_operations_per_minute **")
    print(list_all_df_with_operations_per_minute)
    ### Diviser la liste des dataframes avec operations par minute en DF de Train et de Test
    # concatenation de la liste des dataframes
    if len(list_all_df_with_operations_per_minute) > 1:
        df_with_operations_per_minute = pd.concat(list_all_df_with_operations_per_minute, axis=0).reset_index(drop=True)
        
        #Split dataset into train and test
        X_train_with_operations_per_minute, X_test_with_operations_per_minute = train_test_split(df_with_operations_per_minute, test_size = 0.3)
        ### DataSet de Train
        df_phone_number_with_operations_per_minute = X_train_with_operations_per_minute[['Phone_Number', 'Type_Request']]
        # on drop les colonnes Phone_Number et Type_Request
        X_train_with_operations_per_minute.drop(['Phone_Number', 'Type_Request'],  axis=1, inplace=True)
        print("***dataframe train***")
        print(X_train_with_operations_per_minute.head(5))
        # convertir le dataframe de X_train en une liste
        #list_with_operations_per_minute = X_train_with_operations_per_minute.values.tolist()

        #df_final_operations_per_minute = pd.concat(list_with_operations_per_minute, axis=0).reset_index(drop=True)
        
        outdir_4 = f'{data_folder}/final_file'
        outname_4 = 'X_train_with_operations_per_minute.csv'
        if not os.path.exists(outdir_4):
            os.makedirs(outdir_4, exist_ok=True)

        print('le chemin 4 est : '+outdir_4)
        fullname_4 = os.path.join(outdir_4, outname_4) 
        print('chemin fullname_4: '+fullname_4)
        ### Export DF
        X_train_with_operations_per_minute.to_csv(fullname_4, index=None)
        client_s3.upload_file(os.path.join(outdir_4, outname_4), bucket_name, outname_4)

        # remove files after prediction
        csv_files_outdir_1 = glob2.glob(os.path.join(outdir_1,'*.csv'))
        for file_csv_name_outdir_1 in tqdm(csv_files_outdir_1):
            os.remove(file_csv_name_outdir_1)

        csv_files_outdir_2 = glob2.glob(os.path.join(outdir_2,'*.csv'))
        for file_csv_name_outdir_2 in tqdm(csv_files_outdir_2):
            os.remove(file_csv_name_outdir_2)

        csv_files_outdir_3 = glob2.glob(os.path.join(outdir_3,'*.csv'))
        for file_csv_name_outdir_3 in tqdm(csv_files_outdir_3):
            os.remove(file_csv_name_outdir_3)
        
        csv_files_outdir_4 = glob2.glob(os.path.join(outdir_4,'*.csv'))
        for file_csv_name_outdir_4 in tqdm(csv_files_outdir_4):
            os.remove(file_csv_name_outdir_4)

    return X_train_with_operations_per_minute, df_phone_number_with_operations_per_minute
    #### fin préprocessing


if __name__ == "__main__":
    json_path = os.path.join('./nirra-log-bot','*.json')
    json_files = glob2.glob(os.path.join('./nirra-log-bot','*.json'))
    path_dest = os.path.join('./liberta_leasing')

    process_json('./nirra-log-bot', './liberta_leasing')
    cd ('./liberta_leasing')
    path_file_csv = os.getcwd()
    all_df_list = getListFileCSV(path_file_csv)

    ### Concatener la liste des DataFrames pour avoir un dataframe entier
    df_raw = pd.concat(all_df_list, ignore_index=True)

    ### Sauvegarder le dataframe en local
    outname = 'nirra_log_bot.csv'
    outdir = './static/'
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    fullname = os.path.join(outdir, outname)    
    df_raw.to_csv(fullname, index=None, sep='|')