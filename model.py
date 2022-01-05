import os
import glob
import glob2
import tqdm
import json
import pandas as pd
import numpy as np
import sys
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
from os import chdir as cd
import pickle
from sklearn.decomposition import PCA
from mpl_toolkits import mplot3d
import random
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from preprocessing import preprocessing


# debut entrainement ou definition du modèle
def get_model(df_with_operations_per_minute): 
    list_with_operations_per_minute = df_with_operations_per_minute.values.tolist()
    ### on applique le StandardScaler
    #Scaled_data = StandardScaler().fit_transform(list_resultats)

    # avec fit on prend ce dont on a besoin pour notre jeu de données
    standard_scaler = StandardScaler().fit(list_with_operations_per_minute)
    # ensuite on fait la transformation de ce jeu de données
    transform = standard_scaler.transform(list_with_operations_per_minute)

    ### Application de l'algorithme IsolationForest
    model = IsolationForest(contamination=0.005)
    model_fit = model.fit(list_with_operations_per_minute)
    with open("./static/model.pkl", "wb") as f:
        pickle.dump(model_fit, f)
    
    # on  sauvegarde le standard_scaler pour l'utiliser dans le fichier app.py
    # sur les données de test
    with open("./static/standard_scaler.pkl", "wb") as f_standard:
        pickle.dump(standard_scaler, f_standard)
    return model_fit

if __name__ == "__main__":
    outdir_3 = './static'
    if not os.path.exists(outdir_3):
        os.makedirs(outdir_3, exist_ok=True)

    dataset_is_not_preprared = False
    if dataset_is_not_preprared:
        outname_3 = 'nirra_log_bot.csv'
        fullname_3 = os.path.join(outdir_3, outname_3)   
        df_raw_1 = pd.read_csv(fullname_3, sep='|')
        df_with_operations_per_minute = preprocessing(df_raw_1, outdir_3)
    else:
        outname_3 = 'X_train_with_operations_per_minute.csv'
        print("file outname_3: ")
        print(outname_3)
        fullname_3 = os.path.join(outdir_3, 'final_file', outname_3)
        df_with_operations_per_minute = pd.read_csv(fullname_3)
        print(df_with_operations_per_minute.head(5))
        get_model(df_with_operations_per_minute)

