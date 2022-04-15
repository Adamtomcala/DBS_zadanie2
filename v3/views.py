import psycopg2
import os
from rest_framework.decorators import api_view
from django.http import JsonResponse


def endpoint1(request, match_id):

    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(f"""WITH nested AS (
                    SELECT matches.id, heroes.localized_name, heroes.id as hero,
                    purchase_logs.item_id, items.name, count(*),
                    ROW_NUMBER() OVER (PARTITION BY heroes.id
                                       ORDER BY count(*) DESC, items.name
                                       ) AS row_num ,
                    CASE
                        WHEN player_slot in (0,1,2,3,4) AND radiant_win OR player_slot in (128,129,130,131,132) AND NOT radiant_win THEN True
                        ELSE False
                    END AS winner
                    FROM matches_players_details AS mpd
                    JOIN heroes ON mpd.hero_id = heroes.id
                    JOIN matches ON mpd.match_id = matches.id
                    JOIN purchase_logs ON mpd.id = purchase_logs.match_player_detail_id
                    JOIN items ON purchase_logs.item_id = items.id
                    WHERE matches.id = %s
                    GROUP BY heroes.id, items.id, matches.id, purchase_logs.item_id, items.name, mpd.player_slot)
                SELECT * FROM nested
                WHERE nested.winner is TRUE AND nested.row_num <= 5
                ORDER BY hero, count DESC""" % str(match_id))

    data = cursor.fetchall()

    heroes = []
    for row in data:
        heroes.append(row[2])

    heroes_set = set(heroes)
    heroes = list(heroes_set)
    heroes.sort()

    result = {
        'id': data[0][0],
    }

    it = 0
    final_heores = []

    for i in range(0, len(heroes)):
        hero = heroes[i]
        final_heores.append({
            'id': hero,
            'name': data[it][1],
        })
        purchases = []
        while data[it][2] == hero:
            purchases.append({
                'id': data[it][3],
                'name': data[it][4],
                'count': data[it][5],
            })
            it += 1
            if it == len(data):
                break
        final_heores[len(final_heores) - 1]['top_purchases'] = purchases

    result['heroes'] = final_heores

    return JsonResponse(result, status=200)


def endpoint2(request):

    item = {
        'status': 'ok'
    }
    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)
