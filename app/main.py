from fastapi import FastAPI

from app.routers import router

fast_api_app = FastAPI()

fast_api_app.include_router(router)


# Корневой эндпоинт для проверки
@fast_api_app.get("/")
async def root():
    """
    Корневой маршрут, подтверждающий, что API работает.
    """
    return {"message": "Добро пожаловать!"}
