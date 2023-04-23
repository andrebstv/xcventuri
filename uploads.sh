# Executar a função para realizar o upload dos arquivos para o servidor FTP
upload_ftp

# Verificar se o upload foi realizado com sucesso e registrar no log
if [ $? -eq 0 ]; then
    echo "$(date +"%Y-%m-%d %H:%M:%S") - Upload dos arquivos CSV realizado com sucesso." >> $LOGFILE
else
    echo "$(date +"%Y-%m-%d %H:%M:%S") - Erro ao realizar o upload dos arquivos CSV." >> $LOGFILE
fi
#!/bin/bash

# Definir variáveis com os dados de acesso ao servidor FTP
FTPHOST="82.180.172.8"
FTPUSER="u719047197.andrebstv"
FTPPASS="@ndreF123"

# Definir variáveis com o caminho e nome dos arquivos CSV
CSV1="./apSport.csv"
CSV2="./apLite.csv"
CSV3="./apSerial.csv"
TXTDT="./texto_data.txt"

# Definir variável com o nome do arquivo de log
LOGFILE="./log.txt"

# Função para realizar o upload dos arquivos para o servidor FTP
function upload_ftp {
    # Usar o comando ftp para conectar ao servidor FTP e fazer o upload dos arquivos
    ftp -inv $FTPHOST <<EOF
    user $FTPUSER $FTPPASS
    put $CSV1
    put $CSV2
    put $CSV3
    put $TXTDT
    bye
EOF
}

# Executar a função para realizar o upload dos arquivos para o servidor FTP
if upload_ftp ; then
    echo "$(date +"%Y-%m-%d %H:%M:%S") - Upload dos arquivos CSV realizado com sucesso." >> $LOGFILE
else
    echo "$(date +"%Y-%m-%d %H:%M:%S") - Erro ao realizar o upload dos arquivos CSV. Não foi possível se conectar ao servidor FTP." >> $LOGFILE
fi
