import psycopg2
import re
import os
from psycopg2 import sql, extras
from datetime import datetime, timezone
from user_agents import parse as ua_parse
import logging
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from werkzeug.utils import secure_filename
import asyncio

# --- Конфигурация ---
DATABASE_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5433)),
    'database': os.environ.get('DB_NAME', 'hakaton'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASS', '347620')
}

UPLOAD_LOG_DIRECTORY = './uploaded_logs'
LOCAL_LOG_DIRECTORY = './local_logs'

ALLOWED_EXTENSIONS = {'log'}
BATCH_SIZE = 1000
DEBUG_MODE = os.environ.get('DEBUG', 'False').lower() == 'true'

# Настройка логирования
logging.basicConfig(level=logging.INFO if not DEBUG_MODE else logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Кэши для уменьшения запросов к БД
ip_cache = {}
ua_cache = {}
time_cache = {}
req_type_cache = {}
api_cache = {}
protocol_cache = {}
referrer_cache = {}

app = FastAPI()

def allowed_file(filename):
    """Проверяет допустимость расширения файла.

    Args:
        filename (str): Полное имя файла для проверки.

    Returns:
        bool: True если расширение разрешено, False в противном случае.
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_tables(conn):
    """Создает таблицы в базе данных PostgreSQL при их отсутствии.

    Создает:
        - Таблицы измерений (dim_ip_client, dim_user_agent и др.)
        - Фактовую таблицу local_logs
        - Индексы для оптимизации запросов

    Args:
        conn (psycopg2.connection): Активное соединение с базой данных.

    Raises:
        psycopg2.Error: При ошибках выполнения SQL-запросов.
    """
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
                weekday INTEGER -- 0=Понедельник, 6=Воскресенье
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
                referrer_url TEXT UNIQUE -- Allows NULL implicitly, UNIQUE constraint handles '' vs NULL okay
            )""")
            # Основная таблица логов
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
                referrer_id INTEGER REFERENCES dim_referrer(referrer_id), -- Allows NULL
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

def parse_log_line(line):
    """Парсит строку лога веб-сервера в структурированный формат.

    Поддерживает форматы:
        - NGINX: '192.168.1.1 - - [25/Mar/2023:10:15:32 +0000] "GET /api HTTP/1.1" 200 532...'
        - Альтернативный: '192.168.1.1 [2023-03-25 10:15:32 +0000] "GET /api HTTP/1.1" 200...'

    Args:
        line (str): Строка лога для парсинга.

    Returns:
        dict or None: Словарь с распарсенными данными или None при ошибке.
    """
    nginx_pattern = re.compile(
        r'(\S+) ' # ip_client (1)
        r'\S+ '   # remote logname
        r'\S+ '   # remote user 
        r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] ' # timestamp 
        r'"(\S+) (\S+) (\S+)" ' # request_type, api_path, protocol 
        r'(\d{3}) ' # status_code
        r'(\d+|-) ' # bytes_sent 
        r'"([^"]*|-)" ' # referrer 
        r'"([^"]*)" ' # user_agent
        r'(\d+|-)'  # response_time
    )
    alt_pattern = re.compile(
        r'(\S+) - - ' # ip_client 
        r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \+\d{4})\] ' # timestamp
        r'"(\S+) (\S+) (\S+)" ' # request_type, api_path, protocol
        r'(\d{3}) ' # status_code
        r'(\d+|-) ' # bytes_sent
        r'"([^"]*|-)" ' # referrer
        r'"([^"]*)" ' # user_agent
        r'(\d+|-)'  # response_time
    )

    match = alt_pattern.match(line)
    time_format = '%Y-%m-%d %H:%M:%S %z'
    if not match:
        match = nginx_pattern.match(line)
        time_format = '%d/%b/%Y:%H:%M:%S %z'

    if match:
        try:
            ip_client, timestamp_str, request_type, api_path, protocol, \
            status_code, bytes_sent_str, referrer, user_agent, response_time_str = match.groups()

            timestamp_utc = datetime.strptime(timestamp_str, time_format).astimezone(timezone.utc)

            bytes_sent = int(bytes_sent_str) if bytes_sent_str != '-' else 0
            response_time = int(response_time_str) if response_time_str != '-' else 0
            referrer = None if referrer == '-' or referrer == '' else referrer

            return {
                'ip_client': ip_client,
                'timestamp_utc': timestamp_utc,
                'request_type': request_type,
                'api_path': api_path,
                'protocol': protocol,
                'status_code': int(status_code),
                'bytes_sent': bytes_sent,
                'referrer': referrer,
                'user_agent': user_agent,
                'response_time': response_time
            }
        except ValueError as e:
            logging.warning(f"Ошибка при парсинге или преобразовании строки '{line.strip()}': {e}")
            return None
    else:
        logging.warning(f"Не удалось распарсить строку: '{line.strip()[:100]}...'")
        return None


def get_or_insert_dimension(cursor, cache, table, columns_data):
    """Получает или создает запись в таблице измерений с кэшированием.

    Решает проблему конкурентного выполнения через SAVEPOINT.
    Обрабатывает NULL-значения для разрешенных полей (например, referrer).

    Args:
        cursor (psycopg2.cursor): Курсор для выполнения запросов.
        cache (dict): Кэш для хранения соответствия значений и ID.
        table (str): Название таблицы измерений (например, 'dim_ip_client').
        columns_data (dict): Данные для вставки в формате {column: value}.

    Returns:
        int or None: ID записи или None для NULL-значений.

    Raises:
        psycopg2.Error: При ошибках выполнения SQL-запросов.
        RuntimeError: При критических ошибках после обработки race condition.
    """
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

        except psycopg2.errors.UniqueViolation:
            cursor.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(savepoint_name))
            logging.debug(f"Race condition handled for {table} with value '{main_value}'. Re-selecting.")
            cursor.execute(query_select, (main_value,))
            result = cursor.fetchone()
            if result:
                dim_id = result[0]
                cache[main_value] = dim_id
                return dim_id
            else:
                logging.error(f"CRITICAL: Could not retrieve ID for {table} with '{main_value}' even after handling UniqueViolation race condition.")
                raise RuntimeError(f"Failed to get dimension ID for {table} after race condition.")

        except Exception as e:
            try:
                cursor.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(savepoint_name))
            except Exception as rollback_err:
                logging.error(f"Failed to rollback to savepoint {savepoint_name_str} after another error: {rollback_err}")
            logging.error(f"Error inserting into {table} for {columns_data}: {e}")
            raise



def process_log_lines(conn, lines, batch_buffer):
    """Обрабатывает пакет строк лога и заполняет буфер для вставки.

    Выполняет:
        - Парсинг строк лога
        - Нормализацию данных через таблицы измерений
        - Подготовку данных для пакетной вставки

    Args:
        conn (psycopg2.connection): Соединение с БД.
        lines (list[str]): Список строк лога для обработки.
        batch_buffer (list): Буфер для накопления данных перед вставкой.

    Returns:
        int: Количество успешно обработанных строк.
    """
    processed_lines = 0
    with conn.cursor() as cursor:
        for line in lines:
            log_data = parse_log_line(line.strip())
            if log_data:
                try:
                    ip_client_id = get_or_insert_dimension(cursor, ip_cache, 'dim_ip_client', {'ip_address': log_data['ip_client']})

                    ua = ua_parse(log_data['user_agent'])
                    user_agent_id = get_or_insert_dimension(cursor, ua_cache, 'dim_user_agent', {
                        'user_agent': log_data['user_agent'],
                        'browser': ua.browser.family,
                        'os': ua.os.family,
                        'device_type': 'Mobile' if ua.is_mobile else ('Tablet' if ua.is_tablet else ('PC' if ua.is_pc else 'Other'))
                    })

                    ts = log_data['timestamp_utc']
                    time_id = get_or_insert_dimension(cursor, time_cache, 'dim_time', {
                        'timestamp_utc': ts,
                        'year': ts.year, 'month': ts.month, 'day': ts.day,
                        'hour': ts.hour, 'minute': ts.minute, 'second': ts.second,
                        'weekday': ts.weekday()
                    })

                    request_type_id = get_or_insert_dimension(cursor, req_type_cache, 'dim_request_type', {'request_type': log_data['request_type']})
                    api_id = get_or_insert_dimension(cursor, api_cache, 'dim_api', {'api_path': log_data['api_path']})
                    protocol_id = get_or_insert_dimension(cursor, protocol_cache, 'dim_protocol', {'protocol': log_data['protocol']})

                    referrer_id = None
                    if log_data['referrer'] is not None:
                         referrer_id = get_or_insert_dimension(cursor, referrer_cache, 'dim_referrer', {'referrer_url': log_data['referrer']})

                    batch_buffer.append((
                        ip_client_id,
                        user_agent_id,
                        time_id,
                        request_type_id,
                        api_id,
                        protocol_id,
                        log_data['status_code'],
                        log_data['bytes_sent'],
                        referrer_id,
                        log_data['response_time']
                    ))
                    processed_lines += 1

                except (psycopg2.Error, RuntimeError, ValueError) as e:
                    logging.error(f"Ошибка при обработке строки (dimensions or data): '{line.strip()}' - {e}")
                except Exception as e:
                    logging.error(f"Неожиданная ошибка при обработке строки: '{line.strip()}' - {e}")


    return processed_lines

def insert_batch(conn, batch_buffer):
    """Выполняет пакетную вставку данных в таблицу local_logs.

    Использует execute_values для оптимизации массовой вставки.
    Управляет транзакциями (автокоммит при успехе, откат при ошибке).

    Args:
        conn (psycopg2.connection): Соединение с БД.
        batch_buffer (list): Буфер с данными для вставки.

    Raises:
        psycopg2.Error: При ошибках вставки данных.
    """
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
                logging.error(f"Ошибка при пакетной вставке: {e}")
                conn.rollback()
                raise


async def run_db_operation(func, *args):
    """Выполняет синхронные операции с БД в отдельном потоке.

    Args:
        func (Callable): Синхронная функция для выполнения.
        *args: Аргументы для передаваемой функции.

    Returns:
        Any: Результат выполнения функции.
    """
    return await asyncio.to_thread(func, *args)

async def process_file_async(conn, filepath, is_uploaded_file=False):
    """Асинхронно обрабатывает файл лога.

    Выполняет:
        - Чтение файла
        - Пакетную обработку строк
        - Финализацию транзакций
        - Очистку кэшей
        - Удаление временных файлов (для загруженных)

    Args:
        conn (psycopg2.connection): Соединение с БД.
        filepath (str): Полный путь к файлу лога.
        is_uploaded_file (bool): Флаг загруженного через API файла.

    Returns:
        dict: Результат обработки в формате {'status': ..., 'message': ...}
    """
    filename = os.path.basename(filepath)
    logging.info(f"Начало асинхронной обработки файла: {filename}")
    total_processed = 0
    batch_buffer = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()

            for i in range(0, len(lines), BATCH_SIZE):
                batch_lines = lines[i:i + BATCH_SIZE]
                processed_count = await run_db_operation(process_log_lines, conn, batch_lines, batch_buffer)
                total_processed += processed_count


                if len(batch_buffer) >= BATCH_SIZE:
                     await run_db_operation(insert_batch, conn, batch_buffer)

            if batch_buffer:
                await run_db_operation(insert_batch, conn, batch_buffer)


        logging.info(f"Файл '{filename}' успешно обработан. Обработано {total_processed} строк.")
        return {'status': 'success', 'filename': filename, 'processed': total_processed}

    except FileNotFoundError:
        logging.error(f"Файл '{filename}' не найден по пути: {filepath}")
        return {'status': 'error', 'filename': filename, 'message': 'File not found'}
    except psycopg2.Error as e:
        logging.error(f"Ошибка базы данных при обработке '{filename}': {e}")
        await run_db_operation(conn.rollback)
        return {'status': 'error', 'filename': filename, 'message': f'Database error: {e}'}
    except Exception as e:
        logging.error(f"Ошибка при обработке файла '{filename}': {e}")
        await run_db_operation(conn.rollback)
        return {'status': 'error', 'filename': filename, 'message': f'Processing error: {e}'}
    finally:
        ip_cache.clear(); ua_cache.clear(); time_cache.clear()
        req_type_cache.clear(); api_cache.clear(); protocol_cache.clear(); referrer_cache.clear()
        logging.debug(f"Кэши очищены после обработки {filename}")
        if is_uploaded_file and os.path.exists(filepath):
            try:
                os.remove(filepath)
                logging.info(f"Удален загруженный файл: {filepath}")
            except OSError as e:
                logging.warning(f"Не удалось удалить загруженный файл {filepath}: {e}")


@app.post("/upload/")
async def upload_log_file(file: UploadFile = File(...)):
    """FastAPI эндпоинт для загрузки и обработки лог-файлов.

    Выполняет:
        - Валидацию расширения файла
        - Безопасное сохранение на сервер
        - Запуск обработки файла
        - Удаление временного файла

    Args:
        file (UploadFile): Файл лога, загруженный через форму.

    Returns:
        JSONResponse: Результат обработки в JSON-формате.
    """
    if not file or not file.filename:
        return JSONResponse(content={'error': 'Нет файла в запросе'}, status_code=400)
    if not allowed_file(file.filename):
        return JSONResponse(content={'error': 'Разрешены только файлы с расширением .log'}, status_code=400)

    filename = secure_filename(file.filename)
    os.makedirs(UPLOAD_LOG_DIRECTORY, exist_ok=True)
    filepath = os.path.join(UPLOAD_LOG_DIRECTORY, filename)

    try:
        with open(filepath, "wb") as f:
            content = await file.read()
            f.write(content)
        logging.info(f"Файл '{filename}' успешно загружен в {filepath}.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении загруженного файла '{filename}': {e}")
        return JSONResponse(content={'error': f'Ошибка при сохранении файла: {str(e)}'}, status_code=500)
    finally:
        await file.close()

    conn = None
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        conn.autocommit = False

        result = await process_file_async(conn, filepath, is_uploaded_file=True)

        if result['status'] == 'success':
             return JSONResponse(content={'message': f'Файл "{filename}" успешно обработан. Загружено {result["processed"]} записей.'}, status_code=200)
        else:
             return JSONResponse(content={'error': f'Ошибка при обработке файла "{filename}": {result["message"]}'}, status_code=500)

    except psycopg2.Error as e:
        logging.error(f"Ошибка подключения к базе данных при обработке {filename}: {e}")
        return JSONResponse(content={'error': f'Ошибка базы данных: {str(e)}'}, status_code=500)
    except Exception as e:
         logging.error(f"Неожиданная ошибка в эндпоинте /upload/ для файла {filename}: {e}")
         return JSONResponse(content={'error': f'Внутренняя ошибка сервера: {str(e)}'}, status_code=500)
    finally:
        if conn:
            conn.close()
            logging.info(f"Соединение с БД закрыто после обработки {filename}")



async def main():
    logging.info("Запуск локальной обработки...")
    conn = None
    total_processed_all_files = 0
    processed_files_count = 0
    error_files_count = 0

    try:
        logging.info("Подключение к базе данных для локальной обработки...")
        conn = await run_db_operation(lambda: psycopg2.connect(**DATABASE_CONFIG))
        await run_db_operation(lambda c: setattr(c, 'autocommit', False), conn)
        logging.info("Соединение установлено, autocommit=False.")

        await run_db_operation(create_tables, conn)

        os.makedirs(LOCAL_LOG_DIRECTORY, exist_ok=True)

        log_files = sorted([f for f in os.listdir(LOCAL_LOG_DIRECTORY) if f.endswith('.log')])

        if not log_files:
            logging.warning(f"В каталоге '{LOCAL_LOG_DIRECTORY}' не найдено лог-файлов (*.log).")
            return

        logging.info(f"Найдено {len(log_files)} лог-файлов для локальной обработки.")

        for log_file_name in log_files:
            filepath = os.path.join(LOCAL_LOG_DIRECTORY, log_file_name)
            logging.info(f"--- Начало обработки локального файла: {log_file_name} ---")
            result = await process_file_async(conn, filepath, is_uploaded_file=False) # False here
            if result['status'] == 'success':
                total_processed_all_files += result['processed']
                processed_files_count += 1
                logging.info(f"--- Успешно обработан локальный файл: {log_file_name} ---")
            else:
                error_files_count += 1
                logging.error(f"--- Ошибка при обработке локального файла: {log_file_name}: {result['message']} ---")

        logging.info("--- Локальная обработка завершена ---")
        logging.info(f"Успешно обработано файлов: {processed_files_count}")
        logging.info(f"Файлов с ошибками: {error_files_count}")
        logging.info(f"Всего записей загружено: {total_processed_all_files}")


    except psycopg2.Error as e:
        logging.error(f"Ошибка базы данных в main(): {e}")
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка в main(): {e}")
    finally:
        if conn:
            logging.info("Закрытие соединения с базой данных из main.")
            await run_db_operation(conn.close)


if __name__ == "__main__":
    import uvicorn
    import sys

    asyncio.run(main())
    # # Decide whether to run local processing or the API server
    # if len(sys.argv) > 1 and sys.argv[1] == "--local":
    #     print("Running in local processing mode...")
    #     asyncio.run(main())
    #     print("Local processing finished.")
    # else:
    #     print("Starting FastAPI server on 0.0.0.0:8000...")
    #     # Setup tables before starting the server if needed, or rely on first request/local run
    #     # try:
    #     #    conn = psycopg2.connect(**DATABASE_CONFIG)
    #     #    create_tables(conn)
    #     #    conn.close()
    #     # except Exception as e:
    #     #    logging.warning(f"Could not ensure tables exist before server start: {e}")
    #
    #     uvicorn.run(app, host="0.0.0.0", port=8000)