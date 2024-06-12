# Arquivo Python ETL para os arquivos de dados das mesorregioes

# Importar bibliotecas
import pandas as pd
import csv

# Função Principal
def mesorregiao(path):

    """
    Summary: Função para ler o arquivo de dados das mesorregioes, limpar e retornar um DataFrame.
    Args: path (str): Caminho para o arquivo de dados das mesorregioes.
    Returns: df (DataFrame): DataFrame com os dados das mesorregioes.
    """

    # Lista para armazenar as linhas filtradas
    filtered_lines = []

    # Abre o arquivo CSV original
    with open(path, 'r', encoding='latin-1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        
        copy = False
        for linha in csv_reader:
            if linha and linha[0].startswith("Mesorregião"):
                copy = True
            if copy:
                filtered_lines.append(linha)

    # Converte a lista de linhas filtradas em um DataFrame
    columns = filtered_lines[0] 
    values = filtered_lines[1:] 

    # Converte a lista de dados em um DataFrame
    df = pd.DataFrame(values, columns=columns)

    # Limpa o nome das colunas
    df.columns = [col.split(' ')[0] if '(ha)' in col else col for col in df.columns]

    # Renomeia coluna
    df.rename(columns={'Mesorregião': 'mesoregion'}, inplace=True)

    # Converte as colunas de anos para o tipo correto (int)
    for col in df.columns[1:]:
        df[col] = df[col].str.replace('.', '').astype(int)
    
    # Retorna o DataFrame limpo
    return df

# Executando função principal
df_meso_arroz = mesorregiao('../../data/origin/meso_arroz.csv')
df_meso_milho = mesorregiao('../../data/origin/meso_milho.csv')
df_meso_soja  = mesorregiao('../../data/origin/meso_soja.csv')

# Exportando os arquivos
df_meso_arroz.to_csv('../../data/interim/meso_arroz.csv', index=False)
df_meso_milho.to_csv('../../data/interim/meso_milho.csv', index=False)
df_meso_soja.to_csv('../../data/interim/meso_soja.csv', index=False)