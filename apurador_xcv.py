from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from datetime import datetime as dt

#Fazer a lista de pilotos e links e interar.

#pilotos e IDs sendo carregados
df_pilotos_ids = pd.read_csv("ids_base_pilotos.csv", sep=';')
#formato da apuração
df_apuracao = pd.read_csv("formato_da_apuracao.csv", sep=';')

#loop completo
for i in range(len(df_pilotos_ids)):
    piloto = df_pilotos_ids.loc[i].at['Nome']
    id_ativo = df_pilotos_ids.loc[i].at['ID_XCBrasil']
    data_limite = dt.strptime(df_pilotos_ids.loc[i].at['Data_Entrada'], "%d/%m/%Y")
    html = requests.get("http://xcbrasil.com.br/tracks/world/alltimes/brand:all,cat:0,class:all,xctype:all,club:all,pilot:"+ id_ativo +",takeoff:all&sortOrder=DATE").content
    soup = BeautifulSoup(html, 'html.parser')
    site_limpo = soup.prettify()
    site_limpo.encode(encoding = 'UTF-8', errors = 'ignore')
    lista_data = re.findall("<td class=\"dateString\" valign=\"top\">[\r\n ]+<div>[\r\n ]+(\d\d\/\d\d\/\d\d\d\d)",site_limpo)
    lista_voo = [eval(x) for x in re.findall("(\d*\.?\d+\.\d+).km[\r\n][ ]+<\/td>[\r\n][ ]+<td class=\"OLC", site_limpo)]
    df_voos_data = pd.DataFrame(list(zip(lista_data, lista_voo)),columns =['Data', 'kmOLC'])
    # print(df_voos_data)
    #Aqui já tem os dados completos. Vamos agora dropar os voos acima da data limite.
    for index, row in df_voos_data.iterrows():
        data_voo = dt.strptime(row['Data'], "%d/%m/%Y")
        # data_limite = dt.strptime("01/01/23", "%d/%m/%y")
        if data_limite > data_voo:           
            df_voos_data.drop(index, inplace=True)
     #organiza os voos pela maior distancia
    df_voos_data = df_voos_data.sort_values(by=['kmOLC'], ascending=False, ignore_index=True)
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
    index_da_vez = len(df_apuracao)
    df_apuracao.loc[index_da_vez, 'PILOTO'] = piloto
    df_apuracao.loc[index_da_vez, 'PARAPENTE'] = df_pilotos_ids.loc[i].at['Vela']
    df_apuracao.loc[index_da_vez, 'Distancia OLC KM'] = round(df_voos_data.head(6).sum().at['kmOLC'],1)
    #coloca os demais voos na lista. Sempre checando se ultrapassou.
    for i in range(6):
        dfx = df_voos_data.head(6)
        lenx = len(df_voos_data.head(6))
        if (i < len(df_voos_data.head(6))):
            df_apuracao.iloc[index_da_vez,i+3] = df_voos_data.loc[i].at['kmOLC']
        else:
            df_apuracao.iloc[index_da_vez,i+3] = 0

#organiza por quem ganhou e vamos embora.
df_apuracao = df_apuracao.sort_values(by=['Distancia OLC KM'], ascending=False, ignore_index=True)
df_apuracao.to_csv('apuracao.csv', sep=';') 
print('FIM')

