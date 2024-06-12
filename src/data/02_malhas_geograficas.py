# Arquivo Python ETL para os arquivos de dados das malhas geograficas

# Importar bibliotecas
import pandas       as pd   
import geopandas    as gpd  
import requests     as req  

# URLs da API IBGE
urls = {
    'meso':  'https://servicodados.ibge.gov.br/api/v3/malhas/estados/43?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=mesorregiao',
    'micro': 'https://servicodados.ibge.gov.br/api/v3/malhas/estados/43?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=microrregiao',
    'mun':   'https://servicodados.ibge.gov.br/api/v3/malhas/estados/43?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=municipio'
}

# Função para importar dados da API e salvar como GeoJSON
def save_geojson(url, output_path):
    try:
        # Importação dos dados
        response = req.get(url)
        response.raise_for_status()  # Verifica se houve erro
        data = response.json()

        # GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(data)

        # Salvando GeoDataFrame como GeoJSON
        gdf.to_file(output_path, driver='GeoJSON')
        print(f"Arquivo salvo com sucesso em: {output_path}")
    except req.exceptions.RequestException as e:
        print(f"Erro na requisição da URL {url}: {e}")
    except Exception as e:
        print(f"Erro ao processar dados: {e}")

# Caminhos de destino dos arquivos
output_paths = {
    'meso':  '../../data/geodata/malha_mesorregioes.geojson',
    'micro': '../../data/geodata/malha_microrregioes.geojson',
    'mun':   '../../data/geodata/malha_municipios.geojson'
}

# "Loop for" para processar cada URL e salvar arquivos
for key, url in urls.items():
    save_geojson(url, output_paths[key])
