import datetime
import json
import re
import tqdm
import regex
import os
import glob2
import glob
import pandas as pd
from datetime import datetime


def convertToTimestamp(str, date_format= "%Y-%m-%dT%H:%M:%S.%fZ"):
  '''
  function to convert date to Timestamp
  '''
  element = datetime.datetime.strptime(str, date_format)
  return datetime.datetime.timestamp(element)
  
  
def read_json_insert_csv(root_path, json_file, file_csv):
  data = json.load(json_file)
  df = pd.DataFrame.from_records(data)
  # convert file to csv
  df.to_csv(f'{root_path}/{file_csv}', 
            sep='|', 
            index= None)

  # return 1 fichier csv fer json file
  return df
  
  
def process_json( path_file_json="./nirra-log-bot", dest_path="./liberta_leasing"):
  # créer toute l'aborescence du fichier, crée le chemin
  os.makedirs(dest_path, exist_ok=True) 
  # read all json files

  json_files = glob2.glob(os.path.join(path_file_json,'*.json'))
  json_files = sorted(json_files)
  for file_name in tqdm.tqdm(json_files):
    with open(file_name) as json_file:
      path_file_csv = file_name.replace(".json", ".csv").split("/")[-1]
      read_json_insert_csv(dest_path, json_file, path_file_csv)
      
def getListFileCSV(path_files_csv):
#path = os.getcwd()
  csv_files = glob.glob(os.path.join(path_files_csv, "*.csv"))
  csv_files = sorted(csv_files)
  all_df_list = [] # list of all dataframe
  for f in tqdm.tqdm(csv_files):    
      # read the csv file
      #col_names = ["type", "subtype", "text", "ts", "bot_id"]
      df=pd.read_csv(f, sep="|")
      all_df_list.append(df) 

  return all_df_list
  
def parse_wallet_sms_payload_success(text_type_request):
  ''' la fonction permet de parser les types de requete "Okra WebHook", "Wallet success", 
      "SMS Success" et SMS Payload en object json.
      Elle prend en paramètre le text contenu dans le type de requete,
      elle retourne un objet de type JSON.'''

  pattern = regex.compile(r'\{(?:[^{}]|(?R))*}')
  resul_patt = pattern.findall(text_type_request)
  res = resul_patt[0].replace("\\", " ")
  s = json.loads(res)
  out_dict = {} # dictionnary vide
  for key, value in s.items():
    out_dict[key.strip()] = value # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.


  out_dump = json.dumps(out_dict) # input est un dictionnaire et ça retourne un json sous forme string
  out_wallet_success = json.loads(out_dump) # convertir le string json en object json.
  return out_wallet_success
  
  
  
def parse_providus_transfer_error_function(text_type_request):
  pattern = regex.compile(r'\{(?:[^{}]|(?R))*}')
  resul_patt = pattern.findall(text_type_request)
  if len(resul_patt) != 0:
    res = resul_patt[0].replace("\\", " ")
    s = json.loads(res)
    out_dict = {} # dictionnary vide
    for key, value in s.items():
      out_dict[key.strip()] = value # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.
    out_dump = json.dumps(out_dict) # input est un dictionnaire et ça retourne un json sous forme string
    out_wallet_success = json.loads(out_dump) # convertir le string json en object json.
    return out_wallet_success
  else:
    resultat = 'Transfer to virtual account is not allowed!'
    return resultat
    



# la fonction doit prendre en paramètre quelque chose
def parse_and_concatenate_Leadway_Success_Rows(df_raw):
  '''Cette fonction permet de parser et de concatener le texte qui a LEADWAY SUCCESS
     comme type de requete
     elle prend comme paramètre un dataframe et retourne les valeurs suivantes:
     - un texte concatené
     - l'index de la 1ère ligne qu'on va utiliser ensuite pour l'effacer
     - l'index de la dernière ligne qu'on va utiliser ensuite pour l'effacer '''

  first_index = 0
  last_index = 0
  text_leadway_concat = ''
  for index, row in df_raw.iterrows():  # boucler sur les colonnes de type text
      text_row = row['text']  
      if re.search('LEADWAY SUCCESS', text_row):
        text_leadway_concat = text_row
        first_index = index
        first_index +=1
        new_df = df_raw[first_index:]
        for first_index, new_row in new_df.iterrows():
          xxx = new_row['text']      
          if not xxx.startswith('['):          
            first_index += 1
            text_leadway_concat = text_row + xxx       
          elif xxx.startswith('['):
            last_index = first_index-1
            break


  return text_leadway_concat, first_index, last_index
  
  
  

def parse_Leadway_Success_Row(text_leadway):
  ''' fonction permettant de parser le text concatené pour le type de requet LEADWAY SUCCESS
      elle retourner un dictionnaire.'''
  pattern = regex.compile(r'\{(?:[^{}]|(?R))*}')
  resul_patt = pattern.findall(text_leadway)
  resul_patt[0] = resul_patt[0].replace("\\", "")
  x = resul_patt[0].replace("make,", "")
  y = x.replace('""makeName"', '"makeName"')
  z = json.loads(y)
  vehicleMake = z.get('vehicleMake')
  leadway_dict = {}
  for element in vehicleMake:
    leadway_dict[element['id']] = element['makeName']

  return leadway_dict
  
  
  
  
def parse_Error_Row(error_row):
  error_row = error_row.replace('"', "'")
  pattern = regex.compile(r"{?[a-z :A-Z 0-9\\,=_`']+selfie")
  resul_patt = pattern.findall(error_row)
  res = resul_patt[0].replace("\\", " ")
  res = res.replace("'name'", "name").replace("`", "").replace("'18'", "18").replace("'monthly'", "monthly")
  res = res+'"}'
  res = res.replace("'", '"')
  s = json.loads(res)
  out_error_dict = {} # dictionnary vide
  for key, value in s.items():
    out_error_dict[key.strip()] = value # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.

  out_error_dump = json.dumps(out_error_dict) # input est un dictionnaire et ça retourne un json sous forme string
  out_error_text = json.loads(out_error_dump) # convertir le string json en object json.
  return out_error_text




def parse_Error_Row_selfie_Function(error_row):
  error_row = error_row.replace('"', "'")
  pattern = regex.compile(r"({?[a-z :A-Z 0-9\\,=_`']+)selfie")
  resul_patt = pattern.findall(error_row)
  # checker si la liste contient au moins un element
  if len(resul_patt) !=0 :
    res = resul_patt[0].replace("\\", " ")
    res = res+'"}'
    res = res.replace("'monthly'", "monthly").replace("'", '"').replace('""', '"').replace('"monthly ", "', '"monthly"')
    s = json.loads(res)
    out_error_dict = {} # dictionnary vide
    for key, value in s.items():
      out_error_dict[key.strip()] = value # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.
    out_error_dump = json.dumps(out_error_dict) # input est un dictionnaire et ça retourne un json sous forme string
    out_error_text = json.loads(out_error_dump)
    out_error_text
    return out_error_text
  #si la liste est vide, on la retourne
  else:
    return resul_patt
    
    
    
def parse_Error_Row_selfie_INSERT_INTO_Function(error_row):
  
  error_row = error_row.replace('"', "'")
  pattern = regex.compile(r"({?[a-z :A-Z 0-9\\,=_`']+)selfie")
  resul_patt = pattern.findall(error_row)
  if len(resul_patt) !=0 :
      res = resul_patt[0].replace("\\", " ")
      res = res+'"}'
      res = res.replace("'monthly'", "monthly").replace("'", '"').replace('""', '"').replace("`", '"').replace('monthly, "', '"monthly"').replace('"name"', 'name')
      res = res.replace('"product"', 'product').replace('"loan_amount"', 'loan_amount').replace('"tenor"', 'tenor').replace('"loan_purpose"', 'loan_purpose')
      res = res.replace('"14"', '14').replace('"tenor_type"', 'tenor_type').replace('"monthly"', 'monthly')
      s = json.loads(res)
      out_error_dict = {} # dictionnary vide
      for key, value in s.items():
        out_error_dict[key.strip()] = value # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.
      out_error_dump = json.dumps(out_error_dict) # input est un dictionnaire et ça retourne un json sous forme string
      out_error_text = json.loads(out_error_dump)
      out_error_text
      return out_error_text
    #si la liste est vide, on la retourne
  else:
      return resul_patt
      
      
      
      
def parse_Error_Row_Function (error_row):
  res = []
  pattern = regex.compile(r'\{(?:[^{}]|(?R))*}')
  resul_patt = pattern.findall(error_row)
  for one_res in resul_patt:
    res.append(one_res.replace("\\", " ").replace("`", "").replace("\'", ""))
  list_json = [json.loads(stuff) for stuff in res]
  return list_json  



def df_number_of_operations_per_minute(one_df):
  '''
    Fonction pour calculer le nombre d'opérations par minute
  '''
  one_df_copy = one_df.copy()
  size_df = one_df_copy.shape[0]
  count = 0
  number_of_operations_per_minute = []
  list_max_number_of_operations_per_minute = []
  phone_number_each_df = []
  type_request_each_df = []
  number_of_transactions = []

  #result = pd.DataFrame(columns=['Phone_Number', 'Type_Request', 'Number_operations_per_minute', 'Number_Transactions'])
  for i in range(size_df-1):
    if (one_df_copy.iloc[i]['Timestamp_difference'] <= 60.0000):
      count +=1
      phone_number_each_df.append(one_df_copy.iloc[i]['Phone_Number'])
      type_request_each_df.append(one_df_copy.iloc[i]['Type_Request'])
      number_of_transactions.append(one_df_copy.iloc[i]['Number_Transactions'])
    else:          
      count = 0
      phone_number_each_df.append(one_df_copy.iloc[i+1]['Phone_Number'])
      type_request_each_df.append(one_df_copy.iloc[i+1]['Type_Request'])
      number_of_transactions.append(one_df_copy.iloc[i+1]['Number_Transactions'])
    number_of_operations_per_minute.append(count) 
    #print(max(number_of_operations_per_minute))
    df_type_request = pd.DataFrame(type_request_each_df)
    df_number_of_transactions = pd.DataFrame(number_of_transactions)
    list_max_number_of_operations_per_minute.append(max(number_of_operations_per_minute))
    # on prend la moyenne des opérations par minute
    df_number_of_operations_per_minute = pd.DataFrame(list_max_number_of_operations_per_minute).mean()
    # on crée un dataframe contenant la liste des phone number 
    df_phone_number_each_df = pd.DataFrame(phone_number_each_df) 
    # on fait la concaténation des ces DF
    pdList_consecutive_operations = [df_phone_number_each_df, df_type_request, df_number_of_operations_per_minute, df_number_of_transactions] # List of our dataframes
    df_final_operations_per_minute = pd.concat(pdList_consecutive_operations, axis=1)
    df_final_operations_per_minute.columns=['Phone_Number', 'Type_Request', 'Number_operations_per_minute', 'Number_Transactions']
    # on retourne une seule ligne pour chaque utilisateur car les autres lignes sont similaires   
  return pd.DataFrame(df_final_operations_per_minute.loc[0]).transpose()  



def list_all_df_with_conversion_in_datetime(path):
  '''
  Fonction qui lit les fichiers .csv contenus dans le fichier "files", en crée des Dataframes,
  ensuite fait la différence des Timestamp pour la mettre dans une colonne "Timestamp_difference",
  ensuite chaque ligne de la colonne "ts" est convertie en Datetime. 
  '''
  #path = os.getcwd()
  csv_files_new = glob.glob(os.path.join(path, "*.csv"))
  all_df_list_new = []
  # loop over the list of csv files
  for f in tqdm.tqdm(csv_files_new):    
      # read the csv file
      df_new = pd.read_csv(f)
      df_timestamp = pd.DataFrame(df_new['ts'])
      df_timestamp.sort_values('ts')
      # créer une colonne contenant la différence de timestamp entre 2 lignes
      df_new['Timestamp_difference'] = df_timestamp.diff(axis=0)
      #df_new.sort_values('Timestamp_difference')
      # créer une colonne qui contient la conversion du timestamp en Datetime
      df_new['TS_to_DateTime'] = df_new['ts'].apply(lambda x:datetime.fromtimestamp(x))
      df_new['Number_Transactions'] = df_new.shape[0]
      all_df_list_new.append(df_new)
  return all_df_list_new
  


def get_all_df_diff_ts(new_path):
# use glob to get all the csv files 
# in the folder
  #new_path = os.getcwd()

  list_df_with_diff_ts = []
  number_transactions = []
  all_df_list_with_number_transaction = []

  csv_files = glob.glob(os.path.join(new_path, "*.csv"))
  all_df_list = [] # list of all dataframe
  # loop over the list of csv files
  
  for f in tqdm.tqdm(csv_files):    
      # read the csv file
      df = pd.read_csv(f)  
      all_df_list.append(df)   

  for element in all_df_list:
    element['Number_Transactions'] = element.shape[0] # faire la somme des lignes et mettre dans la colonne Number_Transactions
    all_df_list_with_number_transaction.append(element)# mettre tous les éléments dans une nouvelle liste

  #Faire la différence entre les timestamp et la mettre dans la colonne 'ts_diff'
  for ele_with_num_trans in tqdm.tqdm(all_df_list_with_number_transaction):
    df_ts = pd.DataFrame(ele_with_num_trans['ts'])
    df_ts.sort_values('ts')
    ele_with_num_trans['ts_diff'] = df_ts.diff(axis=0)
    list_df_with_diff_ts.append(ele_with_num_trans)

  return list_df_with_diff_ts


def cancel_files_after_prediction(list_files):
  ''' cette fonction permet d'éliminer dans le serveur après la prédiction
      tous les fichiers stockés pendant le preprocessing'''
  if len(list_files) > 0:
    for one_file in tqdm(list_files):
        os.remove(one_file)

  return "OK"