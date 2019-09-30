from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Review(Base):
    __tablename__ = 'reviews'
    id = Column(Integer, primary_key=True)
    marketplace = Column(String)
    customer_id = Column(String)
    review_id = Column(String)
    product_id = Column(String)
    product_parent = Column(String)
    product_title = Column(String)
    product_category = Column(String)
    star_rating = Column(Integer)
    helpful_votes = Column(Integer)
    total_votes = Column(Integer)
    vine = Column(String)
    verified_purchase = Column(String)
    review_headline = Column(String)
    review_body = Column(String)
    review_date = Column(Date)