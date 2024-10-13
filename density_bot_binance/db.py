from sqlalchemy import Column, Integer, BigInteger, String, Float, Boolean, select, delete, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


# создаем базовый класс для работы с БД
Base = declarative_base()
Session: async_sessionmaker
