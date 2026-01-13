from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis import asyncio as redis

from app.routers import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

    yield {"redis": redis_client}

    await redis_client.aclose()


fast_api_app = FastAPI(lifespan=lifespan)

fast_api_app.include_router(router)


# Корневой эндпоинт для проверки
@fast_api_app.get("/")
async def root():
    """
    Корневой маршрут, подтверждающий, что API работает.
    """
    return {"message": "Добро пожаловать!"}
