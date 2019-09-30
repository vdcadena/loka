from flask import Flask, escape, url_for, request, jsonify, Response
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from config import get_database
from review import Review 

app = Flask(__name__)
db = get_database()
Session = sessionmaker(bind=db)


@app.route("/save_data",methods=["POST"])
def save_data():
    try:
        session = Session()
        data = request.get_json()
        print(data)
        review = Review(**data)
        session.add(review)
        session.commit()
        return Response(status=200)
    except Exception as e:
        print(e)
        return Response(status=500,response="Unable to successfully add the review")

@app.before_first_request
def create_tables():
    # Create tables (if they don't already exist)
    with db.connect() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS reviews"
            "(id SERIAL PRIMARY KEY, marketplace text, customer_id text, "
            "review_id text, product_id text,"
            "product_parent text, product_title text,"
            "product_category text, star_rating integer,"
            "helpful_votes integer, total_votes integer,"
            "vine text, verified_purchase text,"
            "review_headline text, review_body text,"
            "review_date date);"
        )

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True,threaded=True)