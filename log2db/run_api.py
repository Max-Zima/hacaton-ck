"""Загрузка логов в базу по API"""

import uvicorn

from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

# Импортируй app из dashboard.py
from rendering.dashboard import app as dash_app

# Импортируй основной FastAPI app из api.py
from log2db.api import app as fastapi_app

# Оберни Dash-приложение и добавь в FastAPI по маршруту /dashboard
fastapi_app.mount("/dashboard", WSGIMiddleware(dash_app.server))

if __name__ == "__main__":
    
    uvicorn.run("log2db.api:app", host="0.0.0.0", port=8000, reload=True)
