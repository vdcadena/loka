import os
import sqlalchemy

def get_database():
    db = sqlalchemy.create_engine("postgres+pg8000://postgres:welcome1@34.70.134.194:5432/postgres",pool_size=5)
    return db
