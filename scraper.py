# -*- coding: utf-8 -*-
from __future__ import with_statement
from __future__ import print_function
import requests
import os
import pandas as pd
import scraperwiki
from tqdm import tqdm
from datetime import datetime
import sys
import codecs
from chardet.universaldetector import UniversalDetector


def download_file(url, file_name):
    response = requests.get(url, stream=True)
    with open(file_name, "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)
    handle.close()


def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except FileExistsError:
        print("Directory", dirName, "already exists")


def process_file_debentures(url):
    print('Baixando arquivo', url)
    name_file = 'debentures_posicao.csv'
    path_file = os.path.join('downloads', name_file)
    # download file
    download_file(url, path_file)
    # process file
    print('Processando arquivo', name_file)
    
    # convert file to utf-8
    sourceEncoding = "iso-8859-1"
    targetEncoding = "utf-8"
    source = open(path_file)
    target = open(path_file, "w")
    target.write(unicode(source.read(), sourceEncoding).encode(targetEncoding))

    # process file 
    process_file(path_file)
    # remove processed file
    os.remove(path_file)


def process_file(file_path):
    df = pd.read_csv(
        file_path,
        skiprows=2,
        encoding='iso-8859-1',
        sep='\t'
    )

    print('Importing {} items'.format(len(df)))

    # converte para datetime
    df['Data'] =  pd.to_datetime(df['Data'], format='%d/%m/%Y').dt.date
    # formata o campo para float
    df['PU Médio'] = df['PU Médio'].str.replace('.', '')
    df['PU Médio'] = df['PU Médio'].str.replace(',', '.')
    df['PU Médio'] = pd.to_numeric(df['PU Médio'], errors='coerce')
    # formata o campo para float
    df['PU Mínimo'] = df['PU Mínimo'].str.replace('.', '')
    df['PU Mínimo'] = df['PU Mínimo'].str.replace(',', '.')
    df['PU Mínimo'] = pd.to_numeric(df['PU Mínimo'], errors='coerce')
    # formata o campo para float
    df['PU Máximo'] = df['PU Máximo'].str.replace('.', '')
    df['PU Máximo'] = df['PU Máximo'].str.replace(',', '.')
    df['PU Máximo'] = pd.to_numeric(df['PU Máximo'], errors='coerce')    
    # formata o campo para float
    df['% PU da Curva'] = df['% PU da Curva'].str.replace('.', '')
    df['% PU da Curva'] = df['% PU da Curva'].str.replace(',', '.')
    df['% PU da Curva'] = pd.to_numeric(df['% PU da Curva'], errors='coerce')    

    for index, row in df.iterrows():
        data = {
            'data': row['Data'],
            'no_emissor': row['Emissor'],
            'co_ativo': row['Código do Ativo'],
            'isin': row['ISIN'],
            'nu_quantidade': row['Quantidade'],
            'nu_negocios': row['Número de Negócios'],
            'pu_minimo': row['PU Mínimo'],
            'pu_medio': row['PU Médio'],
            'pu_maximo': row['PU Máximo'],
            'pu_curva': row['% PU da Curva']
        }
        scraperwiki.sqlite.save(unique_keys=['data', 'co_ativo', 'isin'], data=data)


def main():
    # create download folder
    create_download_folder()

    dt_ini = datetime(2010, 1, 1)
    dt_ini = datetime(2018, 5, 24)
    dt_ini = dt_ini.strftime("%Y%m%d")
    
    dt_fim = datetime.today()
    dt_fim = dt_fim.strftime("%Y%m%d")
    
    url_base = 'http://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_e.asp'
    url = '{}?op_exc=Nada&emissor=&isin=&ativo=&dt_ini={}&dt_fim={}'.format(url_base, dt_ini, dt_fim)
    
    process_file_debentures(url)


if __name__ == '__main__':
    main()
