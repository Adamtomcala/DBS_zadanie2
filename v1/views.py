from django.db import connection
from django.shortcuts import render
from django.http import JsonResponse


# Create your views here.
def index(request):
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