from fileinput import filename
import imp
from flask import Flask, request, jsonify, render_template
import pickle
import numpy as np
import pandas as pd
from scipy.sparse import data
from werkzeug.utils import secure_filename
from preprocessing import preprocessing
from model import get_model
import os
import glob2
import glob
import tqdm
import ast
import json
from os import chdir as cd
import boto3
from botocore.exceptions import ClientError

# start Flask api
app = Flask(__name__)

# load the pickle model and standard scaler
model = pickle.load(open("./static/model.pkl", "rb"))
standard_scaler = pickle.load(open("./static/standard_scaler.pkl", "rb"))

access_key = 'AKIAWPXBUC7KKNTIR7UI'
secret_access_key =  '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'
bucket_name = 'dct-aws-cloud-labs-pa'

client_s3 = boto3.client(
              's3',
              aws_access_key_id = access_key,
              aws_secret_access_key = secret_access_key
            )

#Home page
@app.route("/")
def index():
  return render_template('index.html')

@app.route("/get_url")
def get_url():
    # Get the service client.
  s3 = boto3.client('s3')

  url = 'http://s3-us-east-1.amazonaws.com/dct-aws-cloud-labs-pa/nirra-log-bot/2021-03-17.json'
  # Generate the URL to get 'key-name' from 'bucket-name'
  url_old = s3.generate_presigned_url(
      ClientMethod='get_object',
      Params={
          'Bucket': '<bucket-name>',
          'Key': '<key-name>'
      }
  )
  return url  


@app.route("/connect_s3")
def connect_to_s3():
  
  # upload files
  data_files_folder = os.path.join(os.getcwd(), 'static/')
  for file in os.listdir(data_files_folder):
    if '.csv' in file: 
      try:
        print('data_files_folder: '+data_files_folder)
        print('uploading file {0}...'+format(file))
        print('join files: '+os.path.join(data_files_folder, file))
        client_s3.upload_file(
           os.path.join(data_files_folder, file),
           bucket_name,
           file
        )

      except ClientError as e:
        print('credential is incorrect')
        print(e)
      except Exception as e:
        print(e)
    
  #Download file
  #client_s3.download_file(bucket_name, '', os.path.join('./'))

  return {}

@app.route("/predict_json", methods=['POST', 'GET'])
def predict_from_json():
  file_json = request.files["filename"].filename
  print('file_json: '+file_json)
  
  #dirname = os.path.dirname(__file__)
  #print('dirname: '+dirname)
  data_files_folder = os.path.join(os.getcwd(), 'static')
  client_s3.upload_file(
           os.path.join(data_files_folder, file_json),
           bucket_name,
           file_json
        )
  filename = secure_filename(file_json)
  print('filename: '+filename)
  outdir = f'./static/'+filename
  print("*** Outdir ***")
  print(outdir)
  if not os.path.exists(outdir):
      os.makedirs(outdir, exist_ok=True)

  #json_files = glob2.glob(os.path.join('./static','*.json'))
  #print("json_files: "+ str(json_files))
  #for file_name in tqdm.tqdm(json_files):
  with open(outdir) as json_file:
    data = json.load(file_json)

  
  #with open(outdir) as json_file:
   #  data = json.load(json_file)

  #data = json.load(json_file)

  #with open(filefullname, 'r') as jsonfile:
  #  data = json.load(jsonfile)
  print('data: '+data)
  df = pd.DataFrame.from_records(data)
  print('df: '+df)
  # convert file to csv
  df.to_csv(f'./static/'+file_json, sep='|',  index= None)
  df_raw = pd.read_csv(f'./static/'+file_json)
  df_res, df_phone = preprocessing(df_raw, './static')
  print("*** df_res ***")
  print(df_res.head())
  print("*** DF phone ***")
  print(df_phone.head())
  pdList = [df_phone, df_res] # list of DF 
  df_final = pd.concat(pdList, axis=1)
  #ss_transformed = standard_scaler.transform(np.array(list(df_res.values)))
  #prediction = model.predict(ss_transformed)
  prediction = model.predict(np.array(list(df_res.values)))
  print("Prediction: ")
  print(prediction)
  df_final["Prediction"] = prediction
  print("DF Final")
  print(df_final.head())
  print("*** anomalies ****")
  print(df_final[df_final['Prediction'] == -1])
  data = []
  data_res = {}
  for index, row in df_final.iterrows():
    data_temp = {}
    data_temp["prediction"] = row["Prediction"]
    data_temp["Phone_number"] = row["Phone_Number"]
    data.append(data_temp)

  data_res["result"] = data
  json_data_str = json.dumps(str(data_res))
  json_data = json.loads(json_data_str) 
  return json_data


@app.route("/predict", methods=['POST', 'GET'])
def predict():  
  file_draw = request.files["filename"].filename
  print("*** file draw ***")
  print(file_draw)
  print('getcwd(): '+os.getcwd())
  data_files_folder = os.path.join(os.getcwd(), 'static')
  print('data_files_folder: '+data_files_folder)
  client_s3.upload_file(
           os.path.join(data_files_folder, file_draw),
           bucket_name,
           file_draw
        )
  filename = secure_filename(file_draw)
  print("*** filename ***")
  print(filename)
  outdir = f'./static/'+filename
  print("*** Outdir ***")
  print(outdir)
  if not os.path.exists(outdir):
      os.makedirs(outdir, exist_ok=True)
  df_raw = pd.read_csv(outdir)
  df_res, df_phone = preprocessing(df_raw, './static')
  print("*** df_res ***")
  print(df_res.head())
  print("*** DF phone ***")
  print(df_phone.head())
  pdList = [df_phone, df_res] # list of DF 
  df_final = pd.concat(pdList, axis=1)
  #ss_transformed = standard_scaler.transform(np.array(list(df_res.values)))
  #prediction = model.predict(ss_transformed)
  prediction = model.predict(np.array(list(df_res.values)))
  print("Prediction: ")
  print(prediction)
  df_final["Prediction"] = prediction
  print("DF Final")
  print(df_final.head())
  print("*** anomalies ****")
  print(df_final[df_final['Prediction'] == -1])
  data = []
  data_res = {}
  for index, row in df_final.iterrows():
    data_temp = {}
    data_temp["prediction"] = row["Prediction"]
    data_temp["Phone_number"] = row["Phone_Number"]
    data.append(data_temp)

  data_res["result"] = data
  json_data_str = json.dumps(str(data_res))
  json_data = json.loads(json_data_str) 
  return json_data


# Launch Flask application
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=3000)