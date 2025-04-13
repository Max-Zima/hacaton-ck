"""Парсинг логов при подготовке к сохранению с БД"""

import re
from datetime import datetime, timezone
import logging


def parse_log_line(line):
    """
    Разбирает строку лога веб-сервера в структурированный формат.
    Поддерживаются два паттерна: для формата NGINX и альтернативного.
    """
    nginx_pattern = re.compile(
        r'(\S+) '                     # ip_client
        r'\S+ '                       # remote logname
        r'\S+ '                       # remote user 
        r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] '  # timestamp 
        r'"(\S+) (\S+) (\S+)" '       # request_type, api_path, protocol 
        r'(\d{3}) '                   # status_code
        r'(\d+|-) '                   # bytes_sent 
        r'"([^"]*|-)" '               # referrer 
        r'"([^"]*)" '                 # user_agent
        r'(\d+|-)'                    # response_time
    )
    alt_pattern = re.compile(
        r'(\S+) - - '                # ip_client 
        r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \+\d{4})\] '  # timestamp
        r'"(\S+) (\S+) (\S+)" '       # request_type, api_path, protocol
        r'(\d{3}) '                  # status_code
        r'(\d+|-) '                  # bytes_sent
        r'"([^"]*|-)" '              # referrer 
        r'"([^"]*)" '                # user_agent
        r'(\d+|-)'                   # response_time
    )
    match = alt_pattern.match(line)
    time_format = '%Y-%m-%d %H:%M:%S %z'
    if not match:
        match = nginx_pattern.match(line)
        time_format = '%d/%b/%Y:%H:%M:%S %z'
    if match:
        try:
            (ip_client, timestamp_str, request_type, api_path, protocol,
             status_code, bytes_sent_str, referrer, user_agent, response_time_str) = match.groups()
            timestamp_utc = datetime.strptime(timestamp_str, time_format).astimezone(timezone.utc)
            bytes_sent = int(bytes_sent_str) if bytes_sent_str != '-' else 0
            response_time = int(response_time_str) if response_time_str != '-' else 0
            referrer = None if referrer in ('-', '') else referrer
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
            logging.warning(f"Ошибка при парсинге строки '{line.strip()}': {e}")
            return None
    else:
        logging.warning(f"Не удалось распарсить строку: '{line.strip()[:100]}...'")
        return None
