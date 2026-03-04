import json
import pandas as pd


def main():
    meteo_json_file = "00-raw-data/meteo/meteo.json"

    with open(meteo_json_file, mode='r') as file:
        meteo_dict: dict = json.load(file)

    print("Meteo")
    # print(meteo_dict.keys())
    # print(type(meteo_dict['hourly']))
    # print(meteo_dict['hourly'].keys())
    # print(meteo_dict['hourly']['time'][:10])
    meteo_df = pd.DataFrame(meteo_dict['hourly'])
    meteo_df['latitude'] = meteo_dict['latitude']
    meteo_df['longitude'] = meteo_dict['longitude']
    meteo_df['elevation'] = meteo_dict['elevation']

    print(meteo_df.head(10))

    print("=" * 50)
    print("INEGI")
    inegi_file = "00-raw-data/inegi/poblacion.json"

    with open(inegi_file, mode='r') as file:
        inegi_dict: dict = json.load(file)

    # print(inegi_dict.keys())
    # print(type(inegi_dict['datos']))
    # print(inegi_dict['datos'][:2])

    inegi_df = pd.DataFrame.from_records(inegi_dict['datos'])
    print(inegi_df.head(10))

if __name__ == "__main__":
    main()