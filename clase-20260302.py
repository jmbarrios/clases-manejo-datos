import pandas as pd


def main() -> None:
    # air_quality_data = pd.read_csv(
    #     "00-raw-data/calidad-aire/contaminantes_2024_sample.csv",
    #     skiprows=9
    # )

    # air_quality_data['id_station'] = air_quality_data['id_station'].astype('category')

    # air_quality_data['date'] = pd.to_datetime(air_quality_data['date'], format='ISO8601')

    dtypes = {
        "id_station": "category",
        "id_parameter": "category",
        "value": "float32",
        "unit": "uint8"
    }

    air_quality_data = pd.read_csv(
        "00-raw-data/calidad-aire/contaminantes_2024_sample.csv",
        dtype=dtypes,
        parse_dates = ['date'],
        skiprows=9
    )

    print(air_quality_data.info())

    print(air_quality_data.head())

if __name__ == '__main__':
    main()