import pandas as pd


def main()->None:
    data: dict[str, list] = {
        "Name": ["Alice", "Bob", "Charlie", "David"],
        "Age": [25, 32, 18, 47],
        "City": ["New York", "Los Angeles", "Chicago", "Houston"]
    }

    df: pd.DataFrame = pd.DataFrame(data)

    print(df)

if __name__ == '__main__':
    main()