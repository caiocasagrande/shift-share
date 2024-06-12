# Arquivo Python ETL para os arquivos de dados das microrregiões

# Importar bibliotecas
import pandas as pd
import csv

# Função Principal para ler arquivos de microrregiões
def microrregiao(path):

    """
    Summary: Função para ler o arquivo de dados das microrregioes, limpar e retornar um DataFrame.
    Args: path (str): Caminho para o arquivo de dados das microrregioes.
    Returns: df (DataFrame): DataFrame com os dados das microrregioes.
    """

    # Lista para armazenar as linhas filtradas
    filtered_lines = []

    # Abre o arquivo CSV original
    with open(path, 'r', encoding='latin-1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        
        copy = False
        for line in csv_reader:
            if line and line[0].startswith("Microrregião"):
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
    df.rename(columns={'Microrregião': 'microregion'}, inplace=True)

    # Remove separador de milhar
    for col in df.columns[1:]:
        df[col] = df[col].str.replace('.', '')

    # Substitui valores "-" por 0
    df.replace('-', 0, inplace=True)

    # Converte colunas de anos para tipo inteiro
    for col in df.columns[1:]:
        df[col] = df[col].astype(int)
    
    # Retorna o DataFrame limpo
    return df

# Executando função principal
df_micro_arroz = microrregiao('../../data/origin/micro_arroz.csv')
df_micro_milho = microrregiao('../../data/origin/micro_milho.csv')
df_micro_soja  = microrregiao('../../data/origin/micro_soja.csv')

# Exportando os arquivos
df_micro_arroz.to_csv('../../data/interim/micro_arroz.csv', index=False)
df_micro_milho.to_csv('../../data/interim/micro_milho.csv', index=False)
df_micro_soja.to_csv('../../data/interim/micro_soja.csv', index=False)
