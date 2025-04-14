"""Загрузка логов в базу по API"""

import uvicorn

if __name__ == "__main__":
    
    uvicorn.run("log2db.api:app", host="0.0.0.0", port=8000, reload=True)
