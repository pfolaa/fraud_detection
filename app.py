from fileinput import filename
from flask import Flask, flash, request, redirect, jsonify, render_template
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
from utils import getListFileCSV

# start Flask api
app = Flask(__name__)

app.secret_key = "secret key"

# load the pickle model and standard scaler
model = pickle.load(open("./static/model.pkl", "rb"))
#standard_scaler = pickle.load(open("./static/standard_scaler.pkl", "rb"))

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


# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['json', 'csv'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# route permettant d'uploader un dossier contenant des fichiers .json
@app.route("/predict_folder_json", methods=['POST', 'GET'])
def predict_from_folder_json():
  if request.method == 'POST':
    if 'files[]' not in request.files:
        flash('No file part')
        return redirect(request.url)

    files = request.files.getlist('files[]')

    upload_dir_file = f'./static/json_files'
    #upload_dir_file = f'.\\static\\json_files\\'
    if not os.path.exists(upload_dir_file):
      os.makedirs(upload_dir_file, exist_ok=True)

    for file in files:
      if file and allowed_file(file.filename):
          filename = secure_filename(file.filename)
          file.save(os.path.join(upload_dir_file, filename))
    flash('File(s) successfully uploaded')
    
    json_files = glob2.glob(os.path.join(upload_dir_file,'*.json'))
    json_files = sorted(json_files)
    for file_name in tqdm.tqdm(json_files):
      with open(file_name, 'r') as f:
        path_file_csv = file_name.replace(".json", ".csv").split("/")[-1]
        print("path_file_csv: "+path_file_csv)
        data = json.load(f)
        df = pd.DataFrame.from_records(data)
        df.to_csv(f'{upload_dir_file}/{path_file_csv}', sep='|', index= None)
            
    all_df_list= getListFileCSV(upload_dir_file)
    df_raw = pd.concat(all_df_list, ignore_index=True)
    print("*** df_raw ***")
    print(df_raw.head())
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
    # remove files json after prediction
    #os.remove(os.path.join(upload_dir_file,'*.json'))
    os.remove(os.path.join(upload_dir_file,'*.csv'))
    os.remove(os.path.join(path_file_csv, '*.json'))
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
    

# route permettant d'uploader un fichier .json
@app.route("/predict_json", methods=['POST', 'GET'])
def predict_from_json():
  if request.method == 'POST':
      # check if the post request has the file part
      if 'file' not in request.files:
          flash('No file part')
          return redirect(request.url)
      file = request.files['file']
      # If the user does not select a file, the browser submits an
      # empty file without a filename.
      if file.filename == '':
          flash('No selected file')
          return redirect(request.url)

      file_json = request.files["filename"].filename
      print('file_json: '+file_json)
      # on recupère le fichier uploadé
      profile = request.files['filename']
      #on récupère le nom du fichier
      file_name = secure_filename(profile.filename)
      # on définit le path où on souhaite sauvegarder le fichier en local
      # mettre \\ si on travaille sur windows
      #upload_dir_file = f'.\\static\\'

      upload_dir_file = f'./static'
      # on sauvegarde le fichier uploadé dans notre file système dans le dossier /static 
      # avec son nom file_name
      profile.save(os.path.join(upload_dir_file, file_name))

      # path du fichier uploadé dans le file system
      # mettre \\ si on travaille sur windows
      #path_file = f'.\\static\\'+file_json

      outdir_1 = f'./static/'+file_json
      print('path_file: '+outdir_1)
      # ouvrir le fichier json
      f = open(outdir_1)
      # on le charge
      data = json.load(f)
      df = pd.DataFrame.from_records(data)
      # convert file to csv
      # mettre \\ si on travaille sur windows
      #outdir_1 = f'.\\static\\csv_files'
      outdir_2 = f'./static'

      df.to_csv(os.path.join(outdir_2, 'final_csv.csv'), sep='|',  index= None)
      #df.to_csv(f'./static/'+file_json, sep='|',  index= None)
      df_raw = pd.read_csv(os.path.join(outdir_2, 'final_csv.csv'), sep="|")
      #df_raw = pd.read_csv(f'./static/'+file_json)
      #df_res, df_phone = preprocessing(df_raw, os.path.join(outdir_2, 'final_csv.csv'))
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
  
  return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


# route permettant d'uploader un fichier .csv
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