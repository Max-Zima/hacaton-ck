"""Локалный модуль работы с БД"""

import os
import asyncio
import logging
import psycopg2
from db import create_tables, run_db_operation
from config import DATABASE_CONFIG, LOCAL_LOG_DIRECTORY
from processor import process_file_async


async def main():
    """Запуск модуля с базой данных"""

    logging.info("Запуск локальной обработки...")
    conn = None
    total_processed_all_files = 0
    processed_files_count = 0
    error_files_count = 0
    try:
        logging.info("Подключение к базе данных для локальной обработки...")
        conn = await run_db_operation(lambda: psycopg2.connect(**DATABASE_CONFIG))
        conn.autocommit = False
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
            logging.info(f"--- Начало обработки файла: {log_file_name} ---")
            result = await process_file_async(conn, filepath, is_uploaded_file=False)
            if result['status'] == 'success':
                total_processed_all_files += result['processed']
                processed_files_count += 1
                logging.info(f"--- Файл {log_file_name} успешно обработан ---")
            else:
                error_files_count += 1
                logging.error(f"--- Ошибка обработки {log_file_name}: {result['message']} ---")
        logging.info("--- Локальная обработка завершена ---")
        logging.info(f"Обработано файлов: {processed_files_count}")
        logging.info(f"Файлов с ошибками: {error_files_count}")
        logging.info(f"Всего записей загружено: {total_processed_all_files}")
    except psycopg2.Error as e:
        logging.error(f"Ошибка базы данных в main(): {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка в main(): {e}")
    finally:
        if conn:
            logging.info("Закрытие соединения с БД из main.")
            await run_db_operation(conn.close)


if __name__ == "__main__":
    import uvicorn, sys
    asyncio.run(main())
    # Если хотите запустить API-сервер, закомментируйте строку выше и раскомментируйте:
    # uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
