# Arquivo Python ETL para os arquivos de dados dos municipios

# Importar bibliotecas
import pandas as pd
import csv

# Função Principal para ler arquivos de Municípios
def municipio(path):

    """
    Summary: Função para ler o arquivo de dados dos municípios, limpar e retornar um DataFrame.
    Args: path (str): Caminho para o arquivo de dados dos municípios.
    Returns: df (DataFrame): DataFrame com os dados dos municípios.
    """

    # Lista para armazenar as linhas filtradas
    filtered_lines = []

    # Abre o arquivo CSV original
    with open(path, 'r', encoding='latin-1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        
        copy = False
        for line in csv_reader:
            if line and line[0].startswith("Município"):
                copy = True
            if copy:
                filtered_lines.append(line)

    # Converte a lista de linhas filtradas em um DataFrame
    columns = filtered_lines[0] 
    values  = filtered_lines[1:] 

    # Converte a lista de dados em um DataFrame
    df = pd.DataFrame(values, columns=columns)

    # Limpa o nome das colunas
    df.columns = [col.split(' ')[0] if '(ha)' in col else col for col in df.columns]

    # Renomeia coluna
    df.rename(columns={'Município': 'municipio'}, inplace=True)

    # Substitui inputs equivocados
    df.replace('...', '-', inplace=True)

    # Remove separador de milhar
    for col in df.columns[4:]:
        df[col] = df[col].str.replace('.', '')

    # Substitui valores "-" por 0
    df.replace('-', 0, inplace=True)

    # Converte colunas de anos para tipo inteiro
    for col in df.columns[4:]:
        df[col] = df[col].astype(int)

    # Converte colunas latitude e longitude para float
    df['latitude']  = df['latitude'].str.replace(',', '.').astype(float)
    df['longitude'] = df['longitude'].str.replace(',', '.').astype(float)

    # Códigos
    df['id_mun'] = df['ibge'].str.slice(0, 6)
    df['id_mun'] = df['id_mun'].astype(int)
    df['ibge']   = df['ibge'].astype(int)

    # Reordenando colunas
    df = df[['ibge', 'id_mun', 'latitude', 'longitude', 'municipio', '2000', '2001', '2002', 
            '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', 
            '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']]

    # Retorna o DataFrame limpo
    return df

# Executando função principal
df_mun_arroz = municipio('../../data/origin/mun_arroz.csv')
df_mun_milho = municipio('../../data/origin/mun_milho.csv')
df_mun_soja  = municipio('../../data/origin/mun_soja.csv')

# Exportando os arquivos
df_mun_arroz.to_csv('../../data/interim/mun_arroz.csv', index=False)
df_mun_milho.to_csv('../../data/interim/mun_milho.csv', index=False)
df_mun_soja.to_csv('../../data/interim/mun_soja.csv', index=False)

