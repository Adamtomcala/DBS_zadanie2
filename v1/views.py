import psycopg2
import os
from rest_framework.decorators import api_view
from django.http import JsonResponse


# Create your views here.
@api_view (['GET'])
def index(request):

    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(f"SELECT VERSION();")

    result1 = cursor.fetchone()

    cursor.execute(f"SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")

    result2 = cursor.fetchone()

    return JsonResponse({'pgsql': {
                                    'version': result1[0],
                                    'dota2_db_size': result2[0]
                                  }
                        }, safe=False)