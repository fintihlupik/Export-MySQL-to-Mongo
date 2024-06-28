from pymongo import MongoClient
import mysql.connector

# Conectarse a MySQL
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="mongo_arbol"
)

# Obtener los datos de MySQL
mysql_cursor = mysql_conn.cursor()

# Obtener los abuelos
mysql_cursor.execute("SELECT * FROM grandparents")
grandparents_data = mysql_cursor.fetchall()

# Obtener los hijos
mysql_cursor.execute("SELECT * FROM children")
children_data = mysql_cursor.fetchall()

# Obtener los nietos
mysql_cursor.execute("SELECT * FROM grandchildren")
grandchildren_data = mysql_cursor.fetchall()

#print(grandchildren_data)

# Conectarse a MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["arbol_basic"]
mongo_collection = mongo_db["mysql_grandparents"]

# Insertar los datos en MongoDB
for grandparent in grandparents_data:
    grandparent_doc = {
        "_id": grandparent[0],
        "name": grandparent[1],
        "age": grandparent[2],
        "children": []
    }

    # Buscar los hijos del abuelo actual
    for child in children_data:
        if child[3] == grandparent[0]:
            child_doc = {
                "_id": child[0],
                "name": child[1],
                "age": child[2],
                "grandchildren": []
            }

            # Buscar los nietos del hijo actual
            for grandchild in grandchildren_data:
                if grandchild[3] == child[0]:
                    grandchild_doc = {
                        "id": grandchild[0],
                        "name": grandchild[1],
                        "age": grandchild[2]
                    }
                    child_doc["grandchildren"].append(grandchild_doc)

            grandparent_doc["children"].append(child_doc)

    mongo_collection.insert_one(grandparent_doc)

# Cerrar las conexiones
mysql_conn.close()
mongo_client.close()

"""
db.mysql_grandparents.findOne( { "children.name": "Alex" } ).age 
- finds the age of Alex´s parent
"""