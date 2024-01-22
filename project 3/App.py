from sqlalchemy import create_engine
from flask import Flask, render_template, jsonify
from config import username, password, hostname, port, db
import pandas as pd
from flask_cors import CORS
from pypika import Query, Table

app = Flask(__name__)
CORS(app)

engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{db}')

@app.route("/api/v1.0/domestic")
def domestic():
   query = "SELECT * FROM domestic"
   conn = engine.connect()
   df = pd.read_sql(query, conn)
   return jsonify(df.to_dict(orient="records"))

@app.route("/api/v1.0/<season>/")
def seasonal_coffee(season):
    season = season.replace("-","/")
    conn = engine.connect()
    domesticdfclean = Table('domesticdfclean')
    query = (Query
        .from_(domesticdfclean)
        .select(domesticdfclean.amount, domesticdfclean.country, domesticdfclean.coffee_type)
        .where(domesticdfclean.season == season)
        .get_sql()
    )
    #query = f"SELECT amount, coffee_type, country FROM domesticdfclean WHERE season = '{season}'"
    df = pd.read_sql(query, conn)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/v1.0/coffee/<year>/")
def annual_coffee(year):
    conn = engine.connect()
    exportsdfclean= Table('exportsdfclean')
    query =(Query
        .from_(exportsdfclean)
        .select(exportsdfclean.amount, exportsdfclean.country)
        .where(exportsdfclean.year == year)
        .get_sql()
    )

#    query = f"SELECT amount, country FROM exportsdfclean WHERE year = '{year}'"
    df = pd.read_sql(query, conn)
    return jsonify(df.to_dict(orient="records"))





if __name__ == "__main__":
    app.run(debug=True)

