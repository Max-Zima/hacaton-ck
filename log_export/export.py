import os
import logging
import psycopg2
import pandas as pd
from log2db.config import DATABASE_CONFIG, EXPORT_DIR


def export_to_dataframe(conn):
    """Извлекает данные из базы данных в pandas DataFrame"""
    query = """
    SELECT
        l.log_id,
        ip.ip_address,
        ua.user_agent,
        ua.browser,
        ua.os,
        ua.device_type,
        t.timestamp_utc,
        t.year, t.month, t.day, t.hour, t.minute, t.second, t.weekday,
        rt.request_type,
        api.api_path,
        proto.protocol,
        l.status_code,
        l.bytes_sent,
        ref.referrer_url,
        l.response_time
    FROM local_logs l
    JOIN dim_ip_client ip ON l.ip_client_id = ip.ip_client_id
    JOIN dim_user_agent ua ON l.user_agent_id = ua.user_agent_id
    JOIN dim_time t ON l.time_id = t.time_id
    JOIN dim_request_type rt ON l.request_type_id = rt.request_type_id
    JOIN dim_api api ON l.api_id = api.api_id
    JOIN dim_protocol proto ON l.protocol_id = proto.protocol_id
    LEFT JOIN dim_referrer ref ON l.referrer_id = ref.referrer_id
    """
    df = pd.read_sql_query(query, conn)
    return df


def export_to_csv(df, filename="exported_logs.csv"):
    """Сохраняет DataFrame в CSV"""
    csv_path = os.path.join(EXPORT_DIR, filename)
    df.to_csv(csv_path, index=False)
    logging.info(f"Данные успешно экспортированы в CSV: {csv_path}")
    return csv_path


def export_to_parquet(df, filename="exported_logs.parquet"):
    """Сохраняет DataFrame в Parquet"""
    parquet_path = os.path.join(EXPORT_DIR, filename)
    df.to_parquet(parquet_path, index=False)
    logging.info(f"Данные успешно экспортированы в Parquet: {parquet_path}")
    return parquet_path


def export_all_csv():
    """Подключается к БД, экспортирует данные в CSV и возвращает путь к файлу"""
    try:
        logging.info("Подключение к базе данных для экспорта в CSV...")
        with psycopg2.connect(**DATABASE_CONFIG) as conn:
            df = export_to_dataframe(conn)
            csv_path = export_to_csv(df)
            logging.info("Экспорт в CSV завершен.")
            return csv_path
    except Exception as e:
        logging.error(f"Ошибка при экспорте в CSV: {e}")
        raise


def export_all_parquet():
    """Подключается к БД, экспортирует данные в Parquet и возвращает путь к файлу"""
    try:
        logging.info("Подключение к базе данных для экспорта в Parquet...")
        with psycopg2.connect(**DATABASE_CONFIG) as conn:
            df = export_to_dataframe(conn)
            parquet_path = export_to_parquet(df)
            logging.info("Экспорт в Parquet завершен.")
            return parquet_path
    except Exception as e:
        logging.error(f"Ошибка при экспорте в Parquet: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export_all_csv()
    export_all_parquet()
