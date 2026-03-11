from sqlalchemy import create_engine
import config


def load_to_db(df):
    print("Loading data to PostgreSQL...")
    conn_str = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    engine = create_engine(conn_str)

    # Save to a table named 'live_flights'
    df.to_sql('live_flights', engine, if_exists='replace', index=False)