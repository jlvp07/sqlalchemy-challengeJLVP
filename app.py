import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# 1. Setup SQLalchemy
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# 2. Create an app, being sure to pass __name__
app = Flask(__name__)


# 3. Define what to do when a user hits the index route
@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    return (
        "Welcome to my Hawaii Climate Analysis API!<br/>"
        "Available Routes:<br/>"
        "/api/v1.0/precipitation<br/>"
        "/api/v1.0/stations<br/>"
        "/api/v1.0/tobs<br/>"
        "/api/v1.0/temp/start/end<br/>"
    )


# 4. Define what to do when a user hits the /precipitation route
@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Server received request for Precipitation API...")
    #Open session for this query
    session = Session(engine)

    # Design a query to retrieve the last 12 months of precipitation data and plot the results
    # Calculate the date 1 year ago from the last data point in the database
    date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    prevDate = dt.datetime.strptime(date , '%Y-%m-%d') - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= prevDate).all()
    results = {date : prcp for date, prcp in results} #List comprehension  

    #Close session to free up the thread
    session.close()
    return jsonify(results)

# 5. Define what to do when a user hits the /stations route
@app.route("/api/v1.0/stations")
def stations():
    print("Server received request for Station API...")
    session = Session(engine)

    results = session.query(Station.station).all()
    #stations = [stations for stations in results]  
    stations = list(np.ravel(results)) 

    session.close()
    return jsonify(stations=stations)

# 6. Define what to do when a user hits the /tobs route
@app.route("/api/v1.0/tobs")
def temperature():
    print("Server received request for Temperature API...")
    session = Session(engine)

    date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    prevDate = dt.datetime.strptime(date , '%Y-%m-%d') - dt.timedelta(days=365)

    stationCountRes = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    mostActive = stationCountRes[0]

    results = session.query(Measurement.tobs).filter(Measurement.station == mostActive[0]).filter(Measurement.date >= prevDate).all()

    temps = list(np.ravel(results)) 
    session.close()
    return jsonify(temps=temps)

# 7. Define what to do when a user hits the /temp/ route(s)
@app.route("/api/v1.0/temp/<start>")
@app.route("/api/v1.0/temp/<start>/<end>")
def calc_temps(start = None, end = None):
    print("Server received request for Date Ranged Temperatures API...")
    session = Session(engine)

    if not end:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).all()
    else:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).filter(Measurement.date <= end).all()

    temps = {'min':results[0][0],
            'avg':results[0][1],
            'max':results[0][2]}

    session.close()
    return jsonify(temps=temps)


if __name__ == "__main__":
    app.run(debug=True)