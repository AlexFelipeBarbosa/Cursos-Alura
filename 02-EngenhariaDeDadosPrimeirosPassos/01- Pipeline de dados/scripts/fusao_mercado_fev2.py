import json 
import csv

# Copiar os arquivos do Linux para Windows
# cp -r /home/alex/Documentos/pipeline_dados /mnt/c/alex/github

from processamento_dados import Dados

path_json = '/home/alex/Documentos/pipeline_dados/data_raw/dados_empresaA.json'
path_csv = '/home/alex/Documentos/pipeline_dados/data_raw/dados_empresaB.csv'


# Extract

dados_empresaA = Dados(path_json, 'json')
print(dados_empresaA.nomes_colunas)
print(dados_empresaA.qtd_linhas)

dados_empresaB = Dados(path_csv, 'csv')
print(dados_empresaB.nomes_colunas)
print(dados_empresaB.qtd_linhas)



# Transform
key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}


dados_empresaB.rename_columns(key_mapping)
print(dados_empresaB.nomes_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(dados_fusao)
print(dados_fusao.nomes_colunas)
print(dados_fusao.qtd_linhas)


# Load
path_dados_combinados = '/home/alex/Documentos/pipeline_dados/data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(path_dados_combinados)