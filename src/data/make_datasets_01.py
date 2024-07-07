# Importando bibliotecas
import pandas as pd

# Função Principal
class DatasetProcessor:
    def __init__(self, df, level, value_name='value'):
        self.df = df
        self.level = level
        self.value_name = value_name
        
    def filter_by_level(self):
        self.df = self.df[self.df['Nível'] == self.level]
        
    def drop_unnecessary_columns(self):
        self.df = self.df.drop(['Nível', 'Unidade da Federação, Município, Mesorregião Geográfica e Microrregião Geográfica'], axis=1)
        
    def rename_columns(self):
        self.df.rename(columns={'Cód.': 'cod_ibge'}, inplace=True)
        
    def melt_dataframe(self):
        self.df = pd.melt(self.df, id_vars=['cod_ibge'], var_name='ano', value_name=self.value_name)
        
    def convert_data_types(self):
        self.df['cod_ibge'] = self.df['cod_ibge'].astype('int64')
        self.df['ano'] = self.df['ano'].astype('int64')
        self.df[self.value_name] = self.df[self.value_name].replace(['-', '...'], 0).astype('int64')
        
    def process_data(self):
        self.filter_by_level()
        self.drop_unnecessary_columns()
        self.rename_columns()
        self.melt_dataframe()
        self.convert_data_types()
        return self.df

class InfoDataFrameProcessor:
    def __init__(self, df):
        self.df = df
    
    def select_columns(self):
        self.df = self.df[['Nível', 'Cód.', 'Unidade da Federação, Município, Mesorregião Geográfica e Microrregião Geográfica']].iloc[1:].copy()
    
    def rename_columns(self):
        self.df.rename(columns={'Unidade da Federação, Município, Mesorregião Geográfica e Microrregião Geográfica': 'nome',
                                     'Nível': 'nivel', 'Cód.': 'cod_ibge'}, inplace=True)
    
    def convert_data_types(self):
        self.df['cod_ibge'] = self.df['cod_ibge'].astype('int64')
    
    def process_data(self):
        self.select_columns()
        self.rename_columns()
        self.convert_data_types()
        return self.df


