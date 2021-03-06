import psycopg2
import os
from rest_framework.decorators import api_view
from django.http import JsonResponse


# Funkcia na nadviazanie spojenia + vratenia vysledku z QUERY
# Funkcia navyse vracia nazvy stlpcov, ktore pouzivam pri formatovani
def get_result_and_columns(query):
    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(query)

    return cursor.fetchall(), [desc[0] for desc in cursor.description]


def top_purchases(request, match_id):

    query = (f"""WITH res AS (
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

    result, name_of_columns = get_result_and_columns(query)

    if not result:
        pass

    item = {
        'id': result[0][0]
    }
    size = len(result)
    heroes = []

    it = 0
    flag = True

    while flag:
        heroes.append({
            name_of_columns[1]: result[it][1],
            name_of_columns[2]: result[it][2],
        })
        items = []
        for i in range(it, size):
            if i == size - 1:
                flag = False
                if result[it][1] == result[i][1]:
                    items.append({
                        name_of_columns[5]: result[i][5],
                        'id': result[i][3],
                        'name': result[i][4],
                    })
                else:
                    heroes[len(heroes) - 1]['top_purchases'] = items
                    items = []
                    items.append({
                        name_of_columns[5]: result[i][5],
                        'id': result[i][3],
                        'name': result[i][4],
                    })
            elif result[it][1] == result[i][1]:
                items.append({
                    name_of_columns[5]: result[i][5],
                    'id': result[i][3],
                    'name': result[i][4],
                })
            else:
                it = i
                break
        heroes[len(heroes) - 1]['top_purchases'] = items

    item['heroes'] = heroes

    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


def ability_usage(request, ability_id):

    query = (f"""WITH res AS (
                 SELECT ab.id, ab.name, h.id AS hero_id, h.localized_name,
                                            CASE
                                                WHEN mpd.player_slot IN (128,129,130,131,132) THEN NOT mt.radiant_win
                                                ELSE mt.radiant_win
                                            END AS winner,
                                            CASE
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.1 THEN '0-9'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.2 THEN '10-19'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.3 THEN '20-29'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.4 THEN '30-39'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.5 THEN '40-49'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.6 THEN '50-59'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.7 THEN '60-69'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.8 THEN '70-79'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 0.9 THEN '80-89'
                                                WHEN CAST(au.time AS DECIMAL)/mt.duration < 1.0 THEN '90-99'
                                                ELSE '100-109'
                                            END AS timee
                                        FROM abilities AS ab
                                        JOIN ability_upgrades AS au
                                            ON ab.id = au.ability_id
                                        JOIN matches_players_details AS mpd
                                            ON mpd.id = au.match_player_detail_id
                                        JOIN matches AS mt
                                            ON mpd.match_id = mt.id
                                        JOIN heroes AS h
                                            ON h.id = mpd.hero_id
                                        WHERE ab.id = %s
            )
            SELECT *
            FROM(
                SELECT DISTINCT *, dense_rank() over(PARTITION BY res2.hero_id, res2.winner ORDER BY res2.cnt DESC) AS "rank"
                        FROM (
                                SELECT res.id, res.name, res.hero_id, res.localized_name, res.winner, res.timee, COUNT(*) AS cnt
                                FROM res
                                GROUP BY (res.id, res.name, res.hero_id,res.localized_name, res.winner, res.timee)
                            ) res2
                ) res3
            WHERE "rank" = 1
            ORDER BY cnt DESC""" % ability_id)

    result, name_of_columns = get_result_and_columns(query)

    if not result:
        pass

    item = {
        name_of_columns[0]: result[0][0],
        name_of_columns[1]: result[0][1],
    }
    size = len(result)
    heroes = []

    it = 0
    flag = True

    heroes = []
    while flag:
        heroes.append({
            'id': result[it][2],
            'name': result[it][3],
        })
        team1 = {}
        team2 = {}
        for i in range(it, size):
            if i == size - 1:
                if result[it][2] == result[i][2]:
                    if result[i][4]:
                        team1 = {
                            'bucket': result[i][5],
                            'count': result[i][6],
                        }
                    else:
                        team2 = {
                            'bucket': result[i][5],
                            'count': result[i][6],
                        }
                else:
                    it = i
                    break
                flag = False
            elif result[it][2] == result[i][2]:
                if result[i][4]:
                    team1 = {
                        'bucket': result[i][5],
                        'count': result[i][6],
                    }
                else:
                    team2 = {
                        'bucket': result[i][5],
                        'count': result[i][6],
                    }
            else:
                it = i
                break

        if len(team1) != 0:
            heroes[len(heroes) - 1]['usage_winners'] = team1
        if len(team2) != 0:
            heroes[len(heroes) - 1]['usage_loosers'] = team2

    item['heroes'] = heroes

    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


def tower_kills(request):

    query = (f""" WITH res AS (SELECT mpd.id, ga.time, mpd.match_id, mpd.hero_id, ga.subtype,
                 ROW_NUMBER() OVER(PARTITION BY mpd.match_id ORDER BY mpd.match_id, ga.time),
                 ROW_NUMBER() OVER(PARTITION BY mpd.match_id, mpd.hero_id ORDER BY mpd.match_id, ga.time),
                 (ROW_NUMBER() OVER(PARTITION BY mpd.match_id ORDER BY mpd.match_id, ga.time)) - 
                 (ROW_NUMBER() OVER(PARTITION BY mpd.match_id, mpd.hero_id ORDER BY mpd.match_id, ga.time)) AS diff
                FROM matches_players_details AS mpd
                JOIN game_objectives AS ga
                    ON ga.match_player_detail_id_1 = mpd.id OR mpd.id = ga.match_player_detail_id_2
                WHERE (ga.match_player_detail_id_1 IS NOT NULL OR ga.match_player_detail_id_2 IS NOT NULL)
                AND ga.subtype = 'CHAT_MESSAGE_TOWER_KILL'
                ORDER BY mpd.match_id, ga.time
                )
                SELECT res3.hero_id, res3.maxx, heroes.localized_name
                FROM (
                        SELECT res2.hero_id, max(res2.r) AS maxx
                        FROM (
                                SELECT *,  ROW_NUMBER() OVER(PARTITION BY res.match_id, res.hero_id, res.diff) AS r
                                FROM res
                                ORDER BY r DESC
                            ) res2
                        GROUP BY res2.hero_id
                        ORDER BY maxx DESC
                    ) res3
                JOIN heroes
                    ON res3.hero_id = heroes.id
                ORDER BY res3.maxx DESC, res3.hero_id""")

    result, name_of_columns = get_result_and_columns(query)

    if not result:
        pass

    heroes = []
    item = {}
    for row in result:
        heroes.append(
            {
                'id': row[0],
                'name': row[2],
                'tower_kills': row[1],
            }
        )
    item['heroes'] = heroes

    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


