import pandas as pd
import glob
import os
from pathlib import Path

#Função que extrai os arquivos
def extrair_dados(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# Função que tranforma os dados
def transformar_dados(df: pd.DataFrame):
    df['TOTAL'] = df['Quantidade'] * df['Venda']
    return df


# Função que carrega os dados
def carregar_dados(df: pd.DataFrame, formatos: list):
    for formato in formatos:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        elif formato == 'parquet':
            df.to_parquet("dados.parquet", index=False)

# Função que define o pipeline com as funções
def pipeline(pasta_entrada: str, formato_saida: str):
    dados = extrair_dados(pasta_entrada)
    dados_transformados = transformar_dados(dados)
    carregar_dados(dados_transformados, formato_saida)
