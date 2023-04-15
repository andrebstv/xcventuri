from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import sqlite3
from datetime import datetime as dt
from unidecode import unidecode
from unicodedata import normalize
import codecs


#Fazer a lista de pilotos e links e interar.

#pilotos e IDs sendo carregados
df_pilotos_ids = pd.read_csv("ids_base_pilotos.csv", sep=';')
#formato da apuração
df_apuracao = pd.read_csv("formato_da_apuracao.csv", sep=';')
voos_validos = pd.DataFrame()
#loop completo
for i in range(len(df_pilotos_ids)):
    piloto = df_pilotos_ids.loc[i].at['Nome']
    vela = df_pilotos_ids.loc[i].at['Vela']
    id_ativo = df_pilotos_ids.loc[i].at['ID_XCBrasil']
    categoria = df_pilotos_ids.loc[i].at['Categoria']
    data_limite = dt.strptime(df_pilotos_ids.loc[i].at['Data_Entrada'], "%d/%m/%Y")
    
    html = requests.get("http://xcbrasil.com.br/tracks/world/alltimes/brand:all,cat:0,class:all,xctype:all,club:all,pilot:"+ id_ativo +",takeoff:all&sortOrder=DATE").content.decode('utf-8', 'ignore')
    #html = unidecode(html)
    soup = BeautifulSoup(html, 'html.parser')
    site_limpo = soup.prettify()
    # site_limpo = unidecode(site_limpo)
    # site_limpo = normalize('NFKD', site_limpo).encode('utf-8', 'ignore').decode('ascii', 'ignore')
    site_limpo = site_limpo.replace("             <span class=\"dateHidden\">", "") #Necessário para remover voos sem datas.
    # site_limpo = site_limpo.replace("UbI", "Uba") #Necessário para remover voos sem datas.
    # site_limpo = site_limpo.replace("UbAA", "Uba") #Necessário para remover voos sem datas.
    # site_limpo = site_limpo.replace("ValArio", "Valerio") #Necessário para remover voos sem datas.
    #Aqui coleta-se os dados.
    lista_id_voo = re.findall("href=\"/flight/([0-9]+)",site_limpo)
    lista_data = re.findall("<td class=\"dateString\" valign=\"top\">[\r\n ]+<div>[\r\n ]+(\d\d\/\d\d\/\d\d\d\d)",site_limpo)
    lista_kmOLC_voo = [eval(x) for x in re.findall("(\d*\.?\d+\.\d+).km[\r\n][ ]+<\/td>[\r\n][ ]+<td class=\"OLC", site_limpo)]
    lista_duracao = re.findall("[\s]+(\d:\d\d)[\s]+<\/td>[\s]+<td class=\"distance\">",site_limpo)
    lista_pontuacaoOLC = [eval(x) for x in re.findall("<td class=\"OLCScore\" nowrap=\"\">[\r\n ]+(\d*\.?\d+\.\d+)",site_limpo)]
    lista_local_voo = re.findall("takeoffTip\.hide\(\)\">[\r\n ]+([A-Z][a-zA-Z ]+)",site_limpo)
    #Monta o dataframe
    df_voos_data = pd.DataFrame(list(zip(lista_id_voo,
                                         lista_data, 
                                         lista_kmOLC_voo,
                                         lista_pontuacaoOLC,
                                         lista_local_voo,
                                         lista_duracao)),columns = ['ID_voo','Data', 'kmOLC','Pontuacao_OLC','local_voo','duracao'])
    #Completa as colunas extras
    df_voos_data['Piloto'] = piloto
    df_voos_data['Categoria'] = categoria
    df_voos_data['Vela'] = vela
    df_voos_data['Data_Coleta'] = dt.today().date()
    df_voos_data['Voo_data_valido'] = True
    df_voos_data['Voo_rampa_valida'] = True
    # Cria uma conexão com o banco de dados pra validar a rampa.
    conn = sqlite3.connect('base_de_dados.db')
    # Cria um cursor para executar comandos SQL
    cursor = conn.cursor()
    # Executa a consulta SELECT com uma cláusula WHERE para verificar se a rampa existe
    for index, row in df_voos_data.iterrows():
        rampa = row['local_voo']
        cursor.execute("SELECT * FROM rampas_validas WHERE nome LIKE ?", (rampa.rstrip()+'%',))
        resultados = cursor.fetchall()
        # Verifica se a consulta retornou alguma linha
        if len(resultados) > 0:
            df_voos_data.at[index, 'Voo_rampa_valida'] = True
        else:
            df_voos_data.at[index, 'Voo_rampa_valida'] = False
            
    
    # df_voos_data.to_sql('tabela_voos', conn, if_exists='append', index=False)
    
    #print(df_voos_data)
    #Aqui já tem os dados completos. Vamos agora dropar os voos acima da data limite.
    for index, row in df_voos_data.iterrows():
        data_voo = dt.strptime(row['Data'], "%d/%m/%Y")
        # data_limite = dt.strptime("01/01/23", "%d/%m/%y")
        if data_limite > data_voo:           
            # df_voos_data.drop(index, inplace=True)
            df_voos_data.at[index, 'Voo_data_valido'] = False
    #organiza os voos pela maior distancia
    df_voos_data = df_voos_data.sort_values(by=['Pontuacao_OLC'], ascending=False, ignore_index=True)
    voos_validos = pd.concat([voos_validos, df_voos_data])

voos_validos = voos_validos.sort_values(by=['Pontuacao_OLC'], ascending=False, ignore_index=True)
voos_validos.reset_index(drop=True)
# Aqui insere-se os voos de cada piloto, que sejam inéditos, no banco de dados.
for index, row in voos_validos.iterrows():
    query = f'''
    INSERT OR IGNORE INTO tabela_voos (ID_voo, Data, kmOLC, Pontuacao_OLC, local_voo, duracao, Piloto, Categoria, Vela, Data_Coleta, Voo_data_valido, Voo_rampa_valida)
    VALUES ({row['ID_voo']}, '{row['Data']}', {row['kmOLC']}, {row['Pontuacao_OLC']}, '{row['local_voo']}', '{row['duracao']}', 
    '{row['Piloto']}', '{row['Categoria']}', '{row['Vela']}', '{row['Data_Coleta']}', {row['Voo_data_valido']}, {row['Voo_rampa_valida']})
    '''
    conn.execute(query)
conn.commit()

# Consulta SQL que seleciona os seis maiores voos do piloto especificado
for j in range(len(df_pilotos_ids)):
    nome_piloto = df_pilotos_ids.loc[j].at['Nome']
    
    query = f'''
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (ORDER BY Pontuacao_OLC DESC) AS row_num
        FROM tabela_voos
        WHERE Piloto = '{nome_piloto}'
        AND Voo_rampa_valida = true AND Voo_data_valido = true 
    ) t
    WHERE row_num <= 6
    '''
    # Carregar os resultados da consulta em um dataframe para melhor manipulação.
    df_do_banco = pd.read_sql_query(query, conn)
    # Agora transfere para o formato da apuração.
    index_da_vez = len(df_apuracao)
    df_apuracao.loc[index_da_vez, 'PILOTO'] = df_pilotos_ids.loc[j].at['Nome']
    df_apuracao.loc[index_da_vez, 'PARAPENTE'] = df_pilotos_ids.loc[j].at['Vela']
    df_apuracao.loc[index_da_vez, 'Distancia OLC KM'] = round(df_do_banco.head(6).sum().at['Pontuacao_OLC'],1)
        #coloca os demais voos na lista. Sempre checando se ultrapassou.
    for i in range(6):
        dfx = df_do_banco.head(6)
        lenx = len(df_do_banco.head(6))
        if (i < len(df_do_banco.head(6))):
            df_apuracao.iloc[index_da_vez,i+3] = df_do_banco.loc[i].at['kmOLC']
        else:
            df_apuracao.iloc[index_da_vez,i+3] = 0
    #   Montar a apuração com os voos por piloto, na estrutura da planilha.
    #               'PILOTO':piloto ,
    #               'PARAPENTE': df_pilotos_ids.loc[i].at['Vela'],
    #               'Distancia OLC KM' : df_voos_data.head(5).sum().at['kmOLC'],.
    #               'Voo 1':0,
    #               'Voo 2':1,
    #               'Voo 3':2,
    #               'Voo 4':3,
    #               'Voo 5':4,
    #               'Voo 6':5}
df_apuracao = df_apuracao.sort_values(by=['Distancia OLC KM'], ascending=False, ignore_index=True)
df_apuracao.to_csv('apuracao.csv', sep=';') 
voos_validos.to_csv('voos_validos.csv', sep=';')
conn.close()
print('FIM')

