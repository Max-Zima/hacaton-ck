"""Загрузка в БД с помощью API"""

import os
import logging
import psycopg2
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from werkzeug.utils import secure_filename
from log2db.config import UPLOAD_LOG_DIRECTORY, ALLOWED_EXTENSIONS, DATABASE_CONFIG
from log2db.processor import process_file_async
import log_export.export as export


app = FastAPI()

app.mount(
    "/html_page",
    StaticFiles(directory="html_page"),
    name="html_page"
)

def allowed_file(filename):
    """Проверяет, соответствует ли расширение имени файла разрешенному."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.get("/", response_class=HTMLResponse)
async def index():
    """Возвращает HTML-страницу с формой загрузки и кнопками экспорта."""
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
    Эндпоинт для загрузки лог-файла.
    Проверяет расширение, сохраняет файл и инициирует его обработку.
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


@app.get("/export/csv")
async def export_csv():
    """
    Эндпоинт для экспорта данных из БД в CSV.
    Вызывает функцию экспорта и возвращает полученный CSV-файл.
    """
    try:
        csv_path = export.export_all_csv()
        return FileResponse(path=csv_path,
                            filename=os.path.basename(csv_path),
                            media_type='text/csv')
    except Exception as e:
        logging.error(f"Ошибка экспорта CSV: {e}")
        return JSONResponse(content={'error': f'Ошибка экспорта CSV: {str(e)}'}, status_code=500)


@app.get("/export/parquet")
async def export_parquet():
    """
    Эндпоинт для экспорта данных из БД в Parquet.
    Вызывает функцию экспорта и возвращает полученный Parquet-файл.
    """
    try:
        parquet_path = export.export_all_parquet()
        return FileResponse(path=parquet_path,
                            filename=os.path.basename(parquet_path),
                            media_type='application/octet-stream')
    except Exception as e:
        logging.error(f"Ошибка экспорта Parquet: {e}")
        return JSONResponse(content={'error': f'Ошибка экспорта Parquet: {str(e)}'}, status_code=500)
