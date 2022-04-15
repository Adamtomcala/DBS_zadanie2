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


def endpoint2(request, match_id):
    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(f"""SELECT ability_id, ability_name, hero_id,
                   hero_localized_name, time_bucket, is_winner, number_of
            FROM
            (SELECT *, ROW_NUMBER() OVER (PARTITION BY hero_localized_name, is_winner
            ORDER BY number_of DESC) row_number
            FROM
            (SELECT DISTINCT *, COUNT(*) OVER
                (PARTITION BY time_bucket, is_winner, hero_localized_name) AS number_of
            FROM
            (SELECT ability_upgrades.ability_id, abilities.name AS ability_name,
            CASE
                WHEN (100.0*ability_upgrades.time / matches.duration) < 10.0 THEN '0-9'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 20.0 THEN '10-19'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 30.0 THEN '20-29'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 40.0 THEN '30-39'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 50.0 THEN '40-49'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 60.0 THEN '50-59'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 70.0 THEN '60-69'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 80.0 THEN '70-79'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 90.0 THEN '80-89'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 100.0 THEN '90-99'
                WHEN (100.0*ability_upgrades.time / matches.duration) < 110.0 THEN '100-109'
            END AS time_bucket,
            hero_id, heroes.localized_name AS hero_localized_name,
            CASE WHEN (matches.radiant_win AND matches_players_details.player_slot < 5) OR
            (NOT matches.radiant_win AND matches_players_details.player_slot > 5) THEN CAST(1 AS BOOL) ELSE
            CAST(0 AS BOOL) END AS is_winner
            FROM matches
            JOIN matches_players_details ON matches_players_details.match_id = matches.id
            JOIN ability_upgrades ON
                matches_players_details.id = ability_upgrades.match_player_detail_id
            JOIN abilities ON ability_upgrades.ability_id = abilities.id
            JOIN heroes ON matches_players_details.hero_id = heroes.id
            WHERE ability_id = %s ) AS ns ) AS ns1 ) AS ns
            WHERE row_number = 1
            ORDER BY hero_id""" % str(match_id))

    data = cursor.fetchall()

    heroes = []
    for row in data:
        heroes.append(row[2])
    heroes_set = set(heroes)
    heroes = list(heroes_set)

    res = {
        'id': data[0][0],
        'name': data[0][1],
        'heroes': heroes
    }
    return JsonResponse(res, status=200)
    it = 0
    h = []
    for i in range(len(heroes)):
        h.append({
            'id': heroes[i],
            'name': data[it][3],
        })
        win, loss = {}, {}
        while data[it][3] == heroes[i]:
            if data[it][5]:
                win = {
                    'bucket': data[it][4],
                    'count': data[it][6],
                }
            else:
                loss = {
                    'bucket': data[it][4],
                    'count': data[it][6],
                }
            it += 1
            if it == len(data):
                break
        h[len(h) - 1]['usage_winners'] = win
        h[len(h) - 1]['usage_loosers'] = loss

    res['heroes'] = h

    return JsonResponse(res, status=200)
