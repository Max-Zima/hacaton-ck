"""Функции по работе с базой данных"""

import logging
import psycopg2
from psycopg2 import sql, extras, errors
import asyncio


def create_tables(conn):
    """Создает таблицы и индексы в базе данных."""
    logging.info("Проверка и создание таблиц...")
    try:
        with conn.cursor() as cursor:
            # Таблица IP-адресов
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_ip_client (
                ip_client_id SERIAL PRIMARY KEY,
                ip_address TEXT UNIQUE NOT NULL
            )""")
            # Таблица User-Agent
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_user_agent (
                user_agent_id SERIAL PRIMARY KEY,
                user_agent TEXT UNIQUE NOT NULL,
                browser TEXT,
                os TEXT,
                device_type TEXT
            )""")
            # Таблица времени
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id SERIAL PRIMARY KEY,
                timestamp_utc TIMESTAMP WITH TIME ZONE UNIQUE NOT NULL,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                minute INTEGER,
                second INTEGER,
                weekday INTEGER
            )""")
            # Таблица типов запросов
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_request_type (
                request_type_id SERIAL PRIMARY KEY,
                request_type TEXT UNIQUE NOT NULL
            )""")
            # Таблица API путей
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_api (
                api_id SERIAL PRIMARY KEY,
                api_path TEXT UNIQUE NOT NULL
            )""")
            # Таблица протоколов
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_protocol (
                protocol_id SERIAL PRIMARY KEY,
                protocol TEXT UNIQUE NOT NULL
            )""")
            # Таблица рефереров
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_referrer (
                referrer_id SERIAL PRIMARY KEY,
                referrer_url TEXT UNIQUE
            )""")
            # Фактовая таблица логов
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS local_logs (
                log_id BIGSERIAL PRIMARY KEY,
                ip_client_id INTEGER NOT NULL REFERENCES dim_ip_client(ip_client_id),
                user_agent_id INTEGER NOT NULL REFERENCES dim_user_agent(user_agent_id),
                time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
                request_type_id INTEGER NOT NULL REFERENCES dim_request_type(request_type_id),
                api_id INTEGER NOT NULL REFERENCES dim_api(api_id),
                protocol_id INTEGER NOT NULL REFERENCES dim_protocol(protocol_id),
                status_code INTEGER,
                bytes_sent BIGINT,
                referrer_id INTEGER REFERENCES dim_referrer(referrer_id),
                response_time INTEGER
            )""")
            # Индексы
            logging.info("Создание индексов...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_time_id ON local_logs (time_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_api_id ON local_logs (api_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_status_code ON local_logs (status_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_ip_client_id ON local_logs (ip_client_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_user_agent_id ON local_logs (user_agent_id)")
        conn.commit()
        logging.info("Создание таблиц и индексов завершено.")
    except psycopg2.Error as e:
        logging.error(f"Ошибка при создании таблиц: {e}")
        conn.rollback()
        raise


def get_or_insert_dimension(cursor, cache, table, columns_data):
    """Получает или создает запись в измерении с использованием кэша."""
    if not columns_data:
        raise ValueError("columns_data dictionary cannot be empty")
    main_column, main_value = next(iter(columns_data.items()))
    id_col_name = f'{table[4:]}_id'
    if main_value is None:
        return None
    if main_value in cache:
        return cache[main_value]
    query_select = sql.SQL("SELECT {id_col} FROM {table} WHERE {main_col} = %s").format(
        id_col=sql.Identifier(id_col_name),
        table=sql.Identifier(table),
        main_col=sql.Identifier(main_column)
    )
    try:
        cursor.execute(query_select, (main_value,))
        result = cursor.fetchone()
    except psycopg2.Error as e:
        logging.error(f"Error selecting from {table} for {main_column}={main_value}: {e}")
        raise

    if result:
        dim_id = result[0]
        cache[main_value] = dim_id
        return dim_id
    else:
        savepoint_name_str = f"sp_insert_{table.replace('dim_', '')}_{abs(hash(main_value)) % 10000}"
        savepoint_name = sql.Identifier(savepoint_name_str)
        try:
            cursor.execute(sql.SQL("SAVEPOINT {}").format(savepoint_name))
            cols = list(columns_data.keys())
            vals = list(columns_data.values())
            placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * len(cols))
            cols_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
            table_sql = sql.Identifier(table)
            id_col_sql = sql.Identifier(id_col_name)
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING {}").format(
                table_sql,
                cols_sql,
                placeholders_sql,
                id_col_sql
            )
            cursor.execute(insert_query, vals)
            new_id = cursor.fetchone()[0]
            cache[main_value] = new_id
            return new_id
        except errors.UniqueViolation:
            cursor.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(savepoint_name))
            logging.debug(f"Race condition handled for {table} with value '{main_value}'. Re-selecting.")
            cursor.execute(query_select, (main_value,))
            result = cursor.fetchone()
            if result:
                dim_id = result[0]
                cache[main_value] = dim_id
                return dim_id
            else:
                logging.error(f"CRITICAL: Не удалось получить ID для {table} с '{main_value}' после обработки UniqueViolation.")
                raise RuntimeError(f"Не удалось получить ID для {table} после race condition.")
        except Exception as e:
            try:
                cursor.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(savepoint_name))
            except Exception as rollback_err:
                logging.error(f"Ошибка отката до savepoint {savepoint_name_str}: {rollback_err}")
            logging.error(f"Ошибка вставки в {table} для {columns_data}: {e}")
            raise


def insert_batch(conn, batch_buffer):
    """Пакетная вставка данных в таблицу local_logs."""
    if batch_buffer:
        insert_count = len(batch_buffer)
        logging.info(f"Вставка пакета из {insert_count} записей...")
        with conn.cursor() as cursor:
            try:
                query = sql.SQL("""
                    INSERT INTO local_logs (
                        ip_client_id, user_agent_id, time_id, request_type_id, api_id,
                        protocol_id, status_code, bytes_sent, referrer_id, response_time
                    ) VALUES %s
                """)
                extras.execute_values(cursor, query.as_string(cursor), batch_buffer, page_size=insert_count)
                conn.commit()
                logging.info(f"Пакет из {insert_count} записей успешно вставлен и транзакция закоммичена.")
                batch_buffer.clear()
            except psycopg2.Error as e:
                logging.error(f"Ошибка пакетной вставки: {e}")
                conn.rollback()
                raise


async def run_db_operation(func, *args):
    """Выполняет синхронную операцию в отдельном потоке."""
    return await asyncio.to_thread(func, *args)
