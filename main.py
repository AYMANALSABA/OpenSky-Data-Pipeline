from ETL.extract import get_live_data
from ETL.transform import clean_data
from ETL.load import load_to_db


def main():
    # 1. Extract
    data = get_live_data()

    if data:
        # 2. Transform
        df = clean_data(data)

        # 3. Load
        load_to_db(df)
        print("Done! Check your database.")
    else:
        print("No data received.")


if __name__ == "__main__":
    main()