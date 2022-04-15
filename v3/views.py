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

    cursor.execute(f"""WITH res AS (
                    SELECT mpd.id, mpd.hero_id, mpd.match_id, h.localized_name
                    FROM matches_players_details AS mpd
                    JOIN heroes AS h
                        ON
                    h.id = mpd.hero_id
                    WHERE mpd.hero_id IN (
                                        SELECT h.id FROM matches_players_details AS mpd
                                        JOIN matches AS mt
                                            ON mpd.match_id = mt.id
                                        JOIN heroes AS h
                                            ON h.id = mpd.hero_id
                                        WHERE mpd.match_id = %s AND 
                                            ((mpd.player_slot IN (0,1,2,3,4) AND mt.radiant_win) 
                                             OR (mpd.player_slot IN (128,129,130,131,132) AND NOT mt.radiant_win))
                                        ) AND mpd.match_id = %s
                            )																	
                SELECT res3.match_id, res3.hero_id AS "id", res3.localized_name AS "name" ,
                       res3.item_id, res3.item_name, res3. cnt AS "count"
                FROM(
                     SELECT *, rank() over(PARTITION BY res2.hero_id ORDER BY res2.cnt DESC, res2.item_name)
                     FROM (	
                            SELECT DISTINCT res.match_id, res.hero_id, res.localized_name,
                            pl.item_id , items.name AS item_name,  COUNT(*) over(PARTITION BY pl.match_player_detail_id, pl.item_id) AS cnt
                            FROM purchase_logs AS pl
                            JOIN res
                                ON pl.match_player_detail_id = res.id
                            JOIN items
                                ON pl.item_id = items.id
                            ORDER BY res.hero_id, cnt DESC
                          ) res2
                     ORDER BY res2.hero_id, res2.cnt DESC
                    ) res3
                WHERE res3.rank <= 5""" % (str(match_id), str(match_id)))

    data = cursor.fetchall()

    heroes = []
    for row in data:
        heroes.append(row[1])

    heroes_set = set(heroes)
    heroes = list(heroes_set)
    heroes.sort()

    result = {
        'id': data[0][0],
    }

    result['heroes'] = heroes
    return JsonResponse(result, json_dumps_params={'indent': 3}, status=200)

    iterator = 0
    final_heores = []

    for i in range(0, len(heroes)):
        hero = heroes[i]
        final_heores.append({
            'id': hero,
            'name': data[iterator][2],
        })
        purchases = []
        while data[iterator][1] == hero:
            purchases.append({
                'id': data[iterator][3],
                'name': data[iterator][4],
                'count': data[iterator][5],
            })
            iterator += 1
        final_heores[len(final_heores) - 1]['top_purchase'] = purchases

    result['heroes'] = final_heores

    return JsonResponse(result, json_dumps_params={'indent': 3}, status=200)




