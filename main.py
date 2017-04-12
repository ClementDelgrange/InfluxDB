import datetime
import random
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError


def connect(user, pwd, dbname, host='localhost', port=8086):
    client = InfluxDBClient(host, port, user, pwd, dbname)
    return client


def create_db(client, dbname, rpname="", rp=None):
    # Create database
    print("Create database: {}".format(dbname))
    client.create_database(dbname)

    # Create a retention policy
    if rp:
        print("Create a retention policy")
        client.create_retention_policy(rpname, rp, 3, default=True)


def delete_database(client, dbname):
    try:
        client.drop_database(dbname)
    except InfluxDBClientError as e:
        print(e)


def insert_data(client, metric, retention_policy, protocol):
    """
    Insertion de 10000 mesures dans la base de données.

    Il s'agit de 5000 mesures température dans 2 pièces et dont la variation
    est tirée aléatoirement.
    """
    now = datetime.datetime.today()
    timeinterval_sec = 15
    total_records = 5000
    value = 20
    series = []

    for i in range(0, total_records):
        past_date = now - datetime.timedelta(seconds=i * timeinterval_sec)
        value += random.uniform(-1, 1)
        point_values = {
                "time": past_date,
                "measurement": metric,
                'fields':  {
                    'value': value,
                },
                'tags': {
                    "piece": rooms_names[0],
                },
            }
        series.append(point_values)
        value += random.uniform(-1, 1)
        point_values = {
                "time": past_date,
                "measurement": metric,
                'fields':  {
                    'value': value,
                },
                'tags': {
                    "piece": rooms_names[1],
                },
            }
        series.append(point_values)

    # Insert series
    client.write_points(series,
                        protocol=protocol,
                        retention_policy=retention_policy)


def count_data(client, metric):
    """
    Compter les mesures dans une série
    """
    query = 'select count(*) from {}'.format(metric)

    result = client.query(query)
    print("Number of mesures : {}".format(result))


def select_data(client, metric):
    """
    Sélectionner les valeurs de mesures dans une série
    """
    query = 'select value from {}'.format(metric)

    result = client.query(query)
    for res in result:
        for r in res:
            print("Results : {}".format(res))


def select(client, query):
    """
    Exécuter une requête InfluxQL sur une base
    """
    try:
        result = client.query(query)
    except InfluxDBClientError as e:
        print(e)
    else:
        for res in result:
            print("Results : ")
            for r in res:
                print(r)


if __name__ == '__main__':
    user = 'root'
    pwd = 'root'
    dbname = 'example'
    metric = 'temperatures'
    retention_policy = 'awesome_policy'
    protocol = 'json'
    rooms_names = ["cuisine",  "salon"]

    client = connect(user, pwd, dbname)
    # delete_database(client, dbname)
    # create_db(client, dbname, retention_policy, '3d')
    # insert_data(client, metric, retention_policy, protocol)
    # count_data(client, metric)
    # select_data(client, metric)

    # req = 'select piece, value from temperatures'
    req = ('select mean(value) from temperatures '
           'where time < now() group by time(30m)')
    select(client, req)
