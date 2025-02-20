import configparser
import json
import logging
import logging.config
import os
import zipfile
from io import BytesIO
from typing import Any, Dict, List

import pandas as pd
import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


def configure_logging(env: str = "staging") -> logging.Logger:
    """
    Configura el sistema de logging utilizando un archivo de configuración YAML.

    Parameters:
    env (str): El entorno para el que se configurará el logger. Puede ser 'staging', 'development', o 'production'.

    Returns:
    logging.Logger: El logger configurado para el entorno especificado.
    """

    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    with open("config/logging.yaml", "rt") as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    return logging.getLogger(env)


logger = configure_logging()


def keys_lower(dictionary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte todas las claves del diccionario a minúsculas de manera recursiva.

    Args:
    dictionary (Dict[str, Any]): El diccionario a procesar.

    Returns:
    Dict[str, Any]: El diccionario con todas las claves en minúsculas.
    """
    return {
        k.lower(): keys_lower(v) if isinstance(v, dict) else v
        for k, v in dictionary.items()
    }


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres de las columnas del DataFrame.

    Args:
    df (pd.DataFrame): El DataFrame a procesar.

    Returns:
    pd.DataFrame: El DataFrame con los nombres de las columnas normalizados.
    """
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("[()]", "", regex=True)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    return df


def rename_columns(
    df: pd.DataFrame, replacements: Dict[str, List[str]]
) -> pd.DataFrame:
    """
    Renombra las columnas del DataFrame basándose en un diccionario de reemplazos.

    Args:
    df (pd.DataFrame): El DataFrame a procesar.
    replacements (Dict[str, List[str]]): Diccionario donde las claves son los nuevos nombres de las columnas
                                          y los valores son listas de nombres antiguos que se deben reemplazar.

    Returns:
    pd.DataFrame: El DataFrame con las columnas renombradas.
    """

    columns = {el: k for k, v in replacements.items() for el in v}
    return df.rename(columns=columns)


def process_dataframe_mineco(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza el tratamiento del DataFrame.

    Args:
    df (pd.DataFrame): El DataFrame a procesar.

    Returns:
    pd.DataFrame: El DataFrame tratado y filtrado.
    """

    # Normaliza los nombres de las columnas del DataFrame
    # - Convierte los nombres a minúsculas
    # - Reemplaza espacios por guiones bajos
    # Esto facilita el acceso a las columnas usando la notación df.column y evita problemas en consultas usando DataFrame.query.
    df = normalize_columns(df)

    # Define un diccionario de reemplazos para renombrar las columnas del DataFrame
    # - Las claves del diccionario son los nuevos nombres estandarizados de las columnas
    # - Los valores son listas de nombres alternativos que pueden aparecer en los datos de entrada (CSV). Por ejemplo, # "mes" puede aparecer como "month", "num_mes"
    # Este paso hace que el proceso ETL sea más robusto, permitiendo manejar posibles variaciones en los nombres de las columnas sin necesidad de cambiar el código.
    replacements = {
        "anio": ["year", "año", "annus", "ano", "iaao"],
        "mes": ["month", "num_mes"],
        "valor": ["observaciones", "observacion", "value"],
    }

    df = rename_columns(df, replacements)

    # 1. Se convierte la columna "valor" a numérico (pd.to_numeric):
    #    - Convierte los valores a string.
    #    - Reemplaza comas por puntos (para manejar decimales en formato europeo).
    #    - Intenta convertir a número; si falla, reemplaza con NaN (errors="coerce").
    # 2. Se eliminas las filas donde la columna "valor" es NaN (método dropna).
    # 3. Se crea una nueva columna "fecha" combinando "mes" y "anio":
    # 4. Se seleccionan solo las columnas "fecha" y "valor".
    # 5. Se ordenan los valores por la columna "fecha" en orden descendente (más recientes primero):
    #    Se convierte "fecha" a formato datetime usando el formato "mes/año".
    # 6. Se redondea los valores de la columna "valor" a 1 decimal.
    
    df["valor"] = pd.to_numeric(
        df["valor"].astype(str).str.strip().str.replace(",", "."), errors="coerce"
    )
    df.dropna(subset=["valor"], inplace=True)

    df["fecha"] = (
        df["mes"].astype(int).astype(str) + "/" + df["anio"].astype(int).astype(str)
    )
    df = df[["fecha", "valor"]]

    df = df.sort_values(
        by="fecha",
        key=lambda col: pd.to_datetime(col, format="%m/%Y"),
        ascending=False,
    )
    df = df.round({"valor": 1})

    return df


def download_zip_in_memory(
    primary_url: str, alternative_url: str, max_retries: int
) -> BytesIO:
    """
    Descarga un archivo ZIP desde la URL proporcionada y lo almacena en memoria.
    Si la descarga desde la URL principal falla, intenta descargar desde una URL alternativa.

    Args:
    primary_url (str): La URL desde la cual descargar el archivo ZIP.
    alternative_url (str): La URL alternativa en caso de que falle la descarga desde la URL principal.
    max_retries (int): El número máximo de reintentos en caso de fallos de conexión.

    Returns:
    BytesIO: El archivo ZIP descargado en memoria.
    """

    # Configuración de la estrategia de reintentos
    retry_stategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry_stategy)

    # Creación de una sesión de requests con el adaptador configurado
    with requests.Session() as session:
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        try:
            response = session.get(primary_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al descargar desde la URL principal: {e}")
            logger.info(
                f"Intentando descargar el archivo ZIP desde la URL alternativa: {alternative_url}"
            )
            try:
                response = session.get(alternative_url)
                response.raise_for_status()
            except requests.exceptions.RequestException as e_alt:
                logger.critical(f"Error al descargar desde la URL alternativa: {e_alt}")
                raise

        return BytesIO(response.content)


def extract_dataframes_from_zip(
    zip_file: BytesIO, dict_indicadores: Dict[str, Any]
) -> Dict[str, pd.DataFrame]:
    """
    Extrae los archivos CSV de un archivo ZIP y devuelve los DataFrames correspondientes sin tratar.

    Args:
    zip_file (BytesIO): El archivo ZIP en memoria.
    dict_indicadores (Dict[str, Any]): El diccionario de indicadores.

    Returns:
    Dict[str, pd.DataFrame]: Un diccionario con los códigos de indicadores como claves y los DataFrames sin tratar como valores.

    Nota:
    Se trabaja en memoria utilizando BytesIO, lo cual tiene varias ventajas:
    - Mayor velocidad al no tener que escribir y leer desde el disco (Hay que tener en cuenta que el uso de memoria puede ser excesivo
    si el archivo ZIP es muy grande).
    - Evita la necesidad de gestionar ficheros temporales.
    - Reducción de la complejidad en el manejo de archivos.
    """

    dataframes = {}
    indicadores_mineco = list(dict_indicadores.keys())

    with zipfile.ZipFile(zip_file) as zippy:
        for item in zippy.infolist():
            cod_file = item.filename.lower().strip().replace(".csv", "")
            if cod_file in indicadores_mineco:
                with zippy.open(item.filename) as csv_file:
                    df = pd.read_csv(csv_file, sep=";", encoding="latin-1")
                    dataframes[cod_file] = df

    return dataframes


def main():
    # Cargar la configuración
    config = configparser.ConfigParser()
    config.read("config/indicadores.properties")

    BSDICE_DL_1 = config["extraction_links"]["bd_download_1"]
    BSDICE_DL_2 = config["extraction_links"]["bd_download_2"]
    DL_MAX_RETRIES = 2

    # Crea el directorio de salida si no existe
    output_dir = config["paths"]["indicadores_folder"]
    os.makedirs(output_dir, exist_ok=True)
    if not os.path.exists(output_dir):
        logger.debug(f"Directorio de salida creado: {output_dir}")
    else:
        logger.debug(f"Directorio de salida ya existe: {output_dir}")

    # Carga el diccionario de indicadores
    with open("data/indicadores.json", "r") as f:
        dict_indicadores = json.load(f)
    dict_indicadores = keys_lower(dict_indicadores)
    logger.info(
        f"Indicadores macroeconómicos a obtener: {list(dict_indicadores.keys())}"
    )

    # Descarga y procesa el archivo ZIP
    zip_file = download_zip_in_memory(
        primary_url=BSDICE_DL_1, alternative_url=BSDICE_DL_2, max_retries=DL_MAX_RETRIES
    )

    if zip_file:
        logger.info("Se ha descargado correctamente el fichero zip con los indicadores")
        dataframes = extract_dataframes_from_zip(zip_file, dict_indicadores)
        processed_dataframes = {
            k: process_dataframe_mineco(v) for k, v in dataframes.items()
        }

        # Guardar los DataFrames procesados en archivos CSV
        for cod_file, df in processed_dataframes.items():
            cod_csa = dict_indicadores.get(cod_file)[0]
            output_path = os.path.join(
                output_dir, f"indicadores_economicos_{cod_csa}.csv"
            )
            df.to_csv(output_path, sep="\t", index=False, quotechar='"')

        logger.info("Se han generado correctamente los ficheros CSV finales")
        # Añadir al log los códigos de los indicadores no encontrados en la base de datos
        codigos_no_encontrados = [
            indicador
            for indicador in dict_indicadores.keys()
            if indicador not in dataframes.keys()
        ]
        if codigos_no_encontrados:
            logger.warning(
                f"Códigos de indicadores no encontrados en la base de datos: {codigos_no_encontrados}"
            )


if __name__ == "__main__":
    main()
