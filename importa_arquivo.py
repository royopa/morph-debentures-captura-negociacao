# -*- coding: utf-8 -*-
from __future__ import print_function, with_statement

import os
import time
from datetime import datetime

import pandas as pd
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki
from chardet.universaldetector import UniversalDetector

import utils


def main():
    # morph.io requires this db filename, but scraperwiki doesn't nicely
    # expose a way to alter this. So we'll fiddle our environment ourselves
    # before our pipeline modules load.
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    file_path = os.path.join("dados.csv")
    df = pd.read_csv(
        file_path,
        skiprows=2,
        encoding='iso-8859-1',
        sep='\t'
    )

    df = df.rename(columns={
        u'Data': "dt_referencia",
        u'Emissor': "emissor",
        u'C\xf3digo do Ativo': "co_ativo",
        u'ISIN': "isin",
        u'Quantidade': "quantidade",
        u'N\xfamero de Neg\xf3cios': "nu_negocios",
        u'PU M\xednimo': "pu_minimo",
        u'PU M\xe9dio': "pu_medio",
        u'PU M\xe1ximo': "pu_maximo",
        u'% PU da Curva': "pu_curva"
    })

    # converte para datetime
    df['dt_referencia'] = pd.to_datetime(
        df['dt_referencia'], format='%d/%m/%Y').dt.date

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

    # salva o arquivo de saída
    file_path = os.path.join('bases', 'debentures_negociacao.csv')
    print('Salvando resultado capturado no arquivo', file_path)
    df.to_csv(file_path, mode='a', index=False)

    #print('Importing {} items'.format(len(df)))

    for index, row in enumerate(df.to_dict('records')):
        # print(f'{index+1} de {len(df)}')
        scraperwiki.sqlite.save(
            unique_keys=['dt_referencia', 'co_ativo'], data=row)

        if index > 0 and index % 1500 == 0:
            print('Aguarda 30 segundos para não dar lock na base')
            time.sleep(30)

    print('{} Registros importados com sucesso'.format(len(df)))


if __name__ == '__main__':
    main()
    time.sleep(60)
    # rename file
    os.rename('scraperwiki.sqlite', 'data.sqlite')
