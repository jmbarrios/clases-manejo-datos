import pandas as pd
from memory_profiler import profile

# @profile
# def load_df(data_file: str, columns: list[str]) -> pd.DataFrame:
#     """ Carga un csv y regresa un dataframe con las columnas deseadas

#     Args:
#         data_file (str): direccion del archivo csv
#         columns (list[str]): lista de nombres de columnas

#     Returns:
#         pd.DataFrame: dataframe con las columnas seleccionas
#     """
#     df = pd.read_csv(data_file)

#     return df[columns]

# @profile
# def load_df(data_file: str, columns: list[str]) -> pd.DataFrame:
#     """ Carga un csv y regresa un dataframe con las columnas deseadas

#     Args:
#         data_file (str): direccion del archivo csv
#         columns (list[str]): lista de nombres de columnas

#     Returns:
#         pd.DataFrame: dataframe con las columnas seleccionas
#     """
#     df = pd.read_csv(data_file, usecols=columns)

#     return df


@profile
def load_df(data_file: str, dtypes: dict[str, str]) -> pd.DataFrame:
    """Carga un csv y regresa un dataframe con las columnas deseadas

    Args:
        data_file (str): direccion del archivo csv
        dtypes (dict[str, str]): diccionario con las columnas que se seleccionan y los tipos de las mismas

    Returns:
        pd.DataFrame: dataframe con las columnas seleccionas
    """
    df = pd.read_csv(data_file, dtype=dtypes, usecols=dtypes.keys())

    return df


@profile
def count_by_estado(data_file: str, chunksize: int = 10_000) -> dict["str", "int"]:
    count: dict[str, int] = {}
    chunk_iter = pd.read_csv(data_file, usecols=["estadomapa"], chunksize=chunksize)

    for chunk in chunk_iter:
        for estado, group in chunk.groupby("estadomapa"):
            count[estado] = count.get(estado, 0) + len(group)

    return count


def main():
    # Datos descargadados desde http://www.conabio.gob.mx/informacion/gis/?vns=gis_root/snib/mamiferos
    mammal_file = "00-raw-data/mamiferos.202503.csv/mamiferos.csv"

    # columnas_de_interes =  [
    #     'estadomapa',
    #     'familiavalida',
    #     'generovalido',
    #     'especievalida',
    #     'latitud',
    #     'longitud'
    # ]

    # tipos = [
    #     'category',
    #     'str',
    #     'str',
    #     'str',
    #     'float32',
    #     'float32'
    # ]

    # dtypes = dict(zip(columnas_de_interes, tipos))

    # df = load_df(mammal_file, columnas_de_interes)
    # df = load_df(mammal_file, dtypes)

    # print(df.head())

    occurrences_by_state = count_by_estado(mammal_file)
    occurrences_by_state_sorted = sorted(
        occurrences_by_state.items(), key=lambda x: x[1], reverse=True
    )
    print(occurrences_by_state_sorted[:10])


if __name__ == "__main__":
    main()
