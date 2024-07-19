from bson import SON
from flask import Flask, request, url_for, redirect, jsonify
from pymongo import MongoClient

# Connect  to MongoDb using connection string obtained from mongosh
client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh"
                     "+2.2.10")
# Getting the database
db = client.kafka_delivery_trip
# Getting the collection
truck_data = db.truck_data
# BookingID
# GpsProvider
# TRANSPORTATION_DISTANCE_IN_KM
# Creating Flask Application instance
app = Flask(__name__)


# Get the details of the trip based on the booking id
@app.route("/trip_details/<BookingID>", methods=['GET'])
def get_trip_details(BookingID):
    print(BookingID, truck_data.find_one({'BookingID': BookingID}), type(truck_data.find_one({'BookingID': BookingID})))
    return jsonify(truck_data.find_one({'BookingID': BookingID}, {'_id': 0}))


# Get The Details Based on The GpsProvider
# This method will give the total numbers of kms travelled per each gps GpsProvider
pipeline = [
    {
        '$group': {
            '_id': '$GpsProvider',
            'count': {'$sum': 1},
            'avg_kms': {'$avg': '$TRANSPORTATION_DISTANCE_IN_KM'},
            'max_kms': {'$max': '$TRANSPORTATION_DISTANCE_IN_KM'},
            'min_kms': {'$min': '$TRANSPORTATION_DISTANCE_IN_KM'},
            'total_kms': {'$sum': '$TRANSPORTATION_DISTANCE_IN_KM'}
        }
    },
    {
        '$project': {
            '_id': 1,
            'count': 1,
            'avg_kms': 1,
            'max_kms': 1,
            'min_kms': 1,
            'total_kms': 1
        }
    },
    {
        '$sort': SON([('_id', 1), ('count', -1)])
    }
]


@app.route('/GpsProvider', methods=['GET', 'POST'])
def get_kms_travelled():
    if request.method == 'POST':
        if not request.get_json() or 'GpsProvider' not in request.get_json():
            return jsonify({'error': 'invalid request'})
        pipeline_for_gps =[
                {
                    '$match': {
                        'GpsProvider': {'$eq': request.get_json()['GpsProvider']}
                    }
                }, {
                '$group': {
                    '_id': '$GpsProvider',
                    'count': {'$sum': 1},
                    'avg_kms': {'$avg': '$TRANSPORTATION_DISTANCE_IN_KM'},
                    'max_kms': {'$max': '$TRANSPORTATION_DISTANCE_IN_KM'},
                    'min_kms': {'$min': '$TRANSPORTATION_DISTANCE_IN_KM'},
                    'total_kms': {'$sum': '$TRANSPORTATION_DISTANCE_IN_KM'}
                }
            },
                {
                    '$project': {
                        '_id': 1,
                        'count': 1,
                        'avg_kms': 1,
                        'max_kms': 1,
                        'min_kms': 1,
                        'total_kms': 1
                    }
                },
                {
                    '$sort': SON([('_id', 1), ('count', -1)])
                }
            ]

        return [data for data in truck_data.aggregate(pipeline_for_gps)][0]
    return [data for data in truck_data.aggregate(pipeline)]


if __name__ == '__main__':
    app.run(debug=True)
