import sqlite3
import pandas as pd
import requests
import re
# 686430 Com violacao, 682799 sem

# with open("texto_dsss.txt", "w", encoding='utf-8', errors='ignore') as arquivo:
#     arquivo.write(html)
    
# Conexão com o banco de dados
conn = sqlite3.connect('base_de_dados.db')
#Violação de Espaço Aéreo

# Consulta SQL
sql = "SELECT * FROM tabela_voos WHERE Voo_data_valido = 1 AND Voo_rampa_valida = 1"

# Transforma o resultado da consulta em um dataframe
df_voos_validos = pd.read_sql_query(sql, conn)
for index, row in df_voos_validos.iterrows():
    url = 'http://xcbrasil.com.br/flight/' + str(df_voos_validos.at[index, 'ID_voo'])
    response = requests.get(url)
    html = response.text
    if len(re.findall(f'''Violação de Espaço Aéreo''',html)) > 0:
        df_voos_validos.at[index, 'Espaco_aereo'] = True
    else:
        df_voos_validos.at[index, 'Espaco_aereo'] = False

# Salva as modificações no banco de dados
df_voos_validos.to_sql('tabela_voos', conn, if_exists='replace', index=False)

# Fecha a conexão com o banco de dados
conn.close()


# print(html)