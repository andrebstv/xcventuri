import sqlite3
import pandas as pd

# Cria uma conexão com o banco de dados
conn = sqlite3.connect('base_de_dados.db')

# Cria um cursor para executar comandos SQL
cursor = conn.cursor()

# Define a estrutura da tabela
cursor.execute('''CREATE TABLE rampas_validas (
                    Id INTEGER,
                    nome TEXT PRIMARY KEY,
                    latitude REAL,
                    longitude REAL
                    )''')

conn.commit()

conn.execute('''CREATE TABLE IF NOT EXISTS tabela_voos
             (ID_voo INT PRIMARY KEY NOT NULL,
              Data TEXT NOT NULL,
              kmOLC REAL NOT NULL,
              Pontuacao_OLC REAL NOT NULL,
              local_voo TEXT NOT NULL,
              duracao TEXT NOT NULL,
              Piloto TEXT NOT NULL,
              Categoria TEXT NOT NULL,
              Vela TEXT NOT NULL,
              Data_Coleta TEXT NOT NULL,
              Voo_data_valido BOOLEAN NOT NULL,
              Voo_rampa_valida BOOLEAN NOT NULL);''')

# Salva as alterações no banco de dados
conn.commit()

# Lê o arquivo CSV e cria um DataFrame com os dados
df = pd.read_csv('Rampas/rampas_validas.csv', sep=';')

# Insere os dados do DataFrame na tabela SQLite
df.to_sql('rampas_validas', conn, if_exists='replace', index=False)


# Solicita ao usuário o nome da rampa a ser verificada
rampa = 'Vila Valerio '

# Executa a consulta SELECT com uma cláusula WHERE para verificar se a rampa existe
cursor.execute("SELECT * FROM rampas_validas WHERE nome = ?", (rampa.rstrip(),))

# Recupera os resultados da consulta
resultados = cursor.fetchall()

# Verifica se a consulta retornou alguma linha
if len(resultados) > 0:
    print('A rampa', rampa, 'existe na tabela.')
else:
    print('A rampa', rampa, 'não existe na tabela.')

# Fecha a conexão com o banco de dados

# Fecha a conexão com o banco de dados
conn.close()
