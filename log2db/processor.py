"""Обработка логов и сохранение в БД"""

import os
import asyncio
import logging
from log2db.parser import parse_log_line
from log2db.db import get_or_insert_dimension, insert_batch, run_db_operation
from user_agents import parse as ua_parse
from log2db.config import BATCH_SIZE
from log2db.cache import ip_cache, ua_cache, time_cache, req_type_cache, api_cache, protocol_cache, referrer_cache


def process_log_lines(conn, lines, batch_buffer):
    """
    Обрабатывает пакет строк лога:
      - Парсит строку
      - Нормализует данные через измерения (dimensions)
      - Добавляет данные в буфер для пакетной вставки
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
                except Exception as e:
                    logging.error(f"Ошибка при обработке строки '{line.strip()}': {e}")
    return processed_lines


async def process_file_async(conn, filepath, is_uploaded_file=False):
    """
    Асинхронно обрабатывает лог-файл:
      - Читает файл
      - Пакетно обрабатывает строки
      - Вставляет данные в БД
      - Очищает кэш и удаляет файл, если требуется
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
    except Exception as e:
        logging.error(f"Ошибка при обработке файла '{filename}': {e}")
        await run_db_operation(conn.rollback)
        return {'status': 'error', 'filename': filename, 'message': f'Processing error: {e}'}
    finally:
        # Очистка кэшей
        ip_cache.clear(); ua_cache.clear(); time_cache.clear()
        req_type_cache.clear(); api_cache.clear(); protocol_cache.clear(); referrer_cache.clear()
        logging.debug(f"Кэши очищены после обработки {filename}")
        if is_uploaded_file and os.path.exists(filepath):
            try:
                os.remove(filepath)
                logging.info(f"Удален загруженный файл: {filepath}")
            except OSError as e:
                logging.warning(f"Не удалось удалить загруженный файл {filepath}: {e}")
