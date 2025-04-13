"""Загрузка в БД с помощью API"""

import os
import logging
import psycopg2
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, HTMLResponse
from werkzeug.utils import secure_filename
from config import UPLOAD_LOG_DIRECTORY, ALLOWED_EXTENSIONS, DATABASE_CONFIG
from processor import process_file_async


app = FastAPI()


def allowed_file(filename):
    """Проверяет, разрешено ли расширение файла."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.get("/", response_class=HTMLResponse)
async def index():
    """
    Возвращает HTML-страницу с формой загрузки лог-файла
    """
    try:
        with open("html_page/index.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except Exception as e:
        logging.error(f"Ошибка чтения index.html: {e}")
        return HTMLResponse(content="<h1>Ошибка загрузки страницы</h1>", status_code=500)


@app.post("/upload/")
async def upload_log_file(file: UploadFile = File(...)):
    """
    FastAPI эндпоинт для загрузки и обработки лог-файла.
      - Валидирует файл
      - Сохраняет его локально
      - Запускает асинхронную обработку
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
        logging.error(f"Ошибка при сохранении файла '{filename}': {e}")
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
        logging.error(f"Ошибка подключения к БД при обработке {filename}: {e}")
        return JSONResponse(content={'error': f'Ошибка базы данных: {str(e)}'}, status_code=500)
    except Exception as e:
         logging.error(f"Неожиданная ошибка в /upload/ для файла {filename}: {e}")
         return JSONResponse(content={'error': f'Внутренняя ошибка сервера: {str(e)}'}, status_code=500)
    finally:
        if conn:
            conn.close()
            logging.info(f"Соединение с БД закрыто после обработки {filename}")
