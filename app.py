from flask import Flask, request, jsonify, render_template
import pickle
import numpy as np
import pandas as pd
from scipy.sparse import data
from werkzeug.utils import secure_filename
from preprocessing import preprocessing
from model import get_model
import os
import glob
import ast
import json
from os import chdir as cd


# start Flask api
app = Flask(__name__)

# load the pickle model and standard scaler
model = pickle.load(open("./static/model.pkl", "rb"))
standard_scaler = pickle.load(open("./static/standard_scaler.pkl", "rb"))

#Home page
@app.route("/")
def index():
  return render_template('index.html')

@app.route("/list_files")
def list_folders():
  print(str(glob.glob(os.path.join(".\static\*"))))
  return {}

@app.route("/predict", methods=['POST', 'GET'])
def predict():  
  file_draw = request.files["filename"].filename
  print("*** file draw ***")
  print(file_draw)
  filename = secure_filename(file_draw)
  print("*** filename ***")
  print(filename)
  outdir = './static'/filename
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
  ss_transformed = standard_scaler.transform(np.array(list(df_res.values)))
  prediction = model.predict(ss_transformed)
  print("Prediction: ")
  print(prediction)
  df_final["Prediction"] = prediction
  print("DF Final")
  print(df_final.head())
  return jsonify({'response': str(prediction),
                'Phone_Number': str(df_phone['Phone_Number'].values.tolist())})


# Launch Flask application
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=3000)