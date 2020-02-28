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
import os


def download_file(url, file_name):
    response = requests.get(url, stream=True)
    with open(file_name, "wb") as handle:
        for data in response.iter_content():
            handle.write(data)
    handle.close()


def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except Exception:
        print("Directory", dirName, "already exists")


def process_file(file_path):
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    df = pd.read_csv(
        file_path,
        skiprows=2,
        encoding='iso-8859-1',
        sep='\t'
    )

    df = df.rename(columns={
        u'Data':"data",
        u'Emissor':"emissor",
        u'C\xf3digo do Ativo':"co_ativo",
        u'ISIN':"isin",
        u'Quantidade':"quantidade",
        u'N\xfamero de Neg\xf3cios':"nu_negocios",
        u'PU M\xednimo':"pu_minimo",
        u'PU M\xe9dio':"pu_medio",
        u'PU M\xe1ximo':"pu_maximo",
        u'% PU da Curva':"pu_curva"
    })
    
    print('Importing {} items'.format(len(df)))

    # converte para datetime
    df['data'] =  pd.to_datetime(df['data'], format='%d/%m/%Y').dt.date
    # formata o campo para float
    df['pu_medio'] = df['pu_medio'].str.replace('.', '')
    df['pu_medio'] = df['pu_medio'].str.replace(',', '.')
    df['pu_medio'] = pd.to_numeric(df['pu_medio'], errors='coerce')
    # formata o campo para float
    df['pu_minimo'] = df['pu_minimo'].str.replace('.', '')
    df['pu_minimo'] = df['pu_minimo'].str.replace(',', '.')
    df['pu_minimo'] = pd.to_numeric(df['pu_minimo'], errors='coerce')
    # formata o campo para float
    df['pu_maximo'] = df['pu_maximo'].str.replace('.', '')
    df['pu_maximo'] = df['pu_maximo'].str.replace(',', '.')
    df['pu_maximo'] = pd.to_numeric(df['pu_maximo'], errors='coerce')
    # formata o campo para float
    df['pu_curva'] = df['pu_curva'].str.replace('.', '')
    df['pu_curva'] = df['pu_curva'].str.replace(',', '.')
    df['pu_curva'] = pd.to_numeric(df['pu_curva'], errors='coerce')

    for index, row in df.iterrows():
        data = {
            'data': row['data'],
            'no_emissor': row['emissor'],
            'co_ativo': row['co_ativo'],
            'isin': row['isin'],
            'nu_quantidade': row['quantidade'],
            'nu_negocios': row['nu_negocios'],
            'pu_minimo': row['pu_minimo'],
            'pu_medio': row['pu_medio'],
            'pu_maximo': row['pu_maximo'],
            'pu_curva': row['pu_curva']
        }
        scraperwiki.sqlite.save(unique_keys=['data', 'co_ativo', 'isin'], data=data)


def main():
    # create download folder
    create_download_folder()

    dt_ini = datetime(2000, 1, 1)
    dt_ini = dt_ini.strftime("%Y%m%d")

    dt_fim = datetime.today()
    dt_fim = dt_fim.strftime("%Y%m%d")

    url_base = 'http://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_e.asp'
    url = '{}?op_exc=Nada&emissor=&isin=&ativo=&dt_ini={}&dt_fim={}'.format(url_base, dt_ini, dt_fim)

    process_file(url)

    # rename file
    os.rename('scraperwiki.sqlite', 'data.sqlite')


if __name__ == '__main__':
    main()
