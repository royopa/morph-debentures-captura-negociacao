# -*- coding: utf-8 -*-
from __future__ import with_statement
import requests
import os
import pandas as pd
import scraperwiki
from tqdm import tqdm
from datetime import datetime
import sys
import codecs
from chardet.universaldetector import UniversalDetector


def get_encoding_type(current_file):
    detector.reset()
    for line in file(current_file):
        detector.feed(line)
        if detector.done: break
    detector.close()
    return detector.result['encoding']


def convertFileBestGuess(filename):
    targetFormat = 'utf-8'
    outputDir = os.path.join('downloads')
    detector = UniversalDetector()
    sourceFormats = ['ascii', 'iso-8859-1']
    for format in sourceFormats:
        try:
            with codecs.open(fileName, 'rU', format) as sourceFile:
                writeConversion(sourceFile)
                print('Done.')
                return
        except UnicodeDecodeError:
            pass


def convertFileWithDetection(fileName):
    targetFormat = 'utf-8'
    outputDir = os.path.join('downloads')
    detector = UniversalDetector()
    print("Converting '" + fileName + "'...")
    format = get_encoding_type(fileName)
    try:
        with codecs.open(fileName, 'rU', format) as sourceFile:
            writeConversion(sourceFile)
            print('Done.')
            return
    except UnicodeDecodeError:
        pass

    print("Error: failed to convert '" + fileName + "'.")


def writeConversion(file):
    with codecs.open(outputDir + '/' + fileName, 'w', targetFormat) as targetFile:
        for line in file:
            targetFile.write(line)


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
    name_file = 'debentures_posicao.xls'
    path_file = os.path.join('downloads', name_file)
    # download file
    download_file(url, path_file)
    # process file
    print('Processando arquivo', name_file)
    # convert file to UTF-8    
    convertFileWithDetection(path_file)
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
