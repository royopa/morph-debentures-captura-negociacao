# -*- coding: utf-8 -*-
from __future__ import print_function, with_statement

import os
from datetime import datetime

import importa_arquivo
import utils


def process_file(url):
    file_path = os.path.join("dados.csv")

    if utils.download(url, None, file_path) is False:
        print("Erro ao baixar arquivo")
        return False
    print('Iniciando importação')

    importa_arquivo.main()


def main():
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
    utils.prepare_download_folder('bases')
    utils.prepare_download_folder('downloads')

    dt_ini = datetime(1990, 1, 1)
    dt_ini = dt_ini.strftime("%Y%m%d")

    dt_fim = datetime.today()
    dt_fim = dt_fim.strftime("%Y%m%d")

    url_base = 'http://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_e.asp'
    url = '{}?op_exc=Nada&emissor=&isin=&ativo=&dt_ini={}&dt_fim={}'.format(
        url_base, dt_ini, dt_fim)

    process_file(url)


if __name__ == '__main__':
    main()
