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


def endpoint1(request, match_id):

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
        name_of_columns[0]: result[0][0]
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
                        name_of_columns[3]: result[i][3],
                        name_of_columns[4]: result[i][4],
                        name_of_columns[5]: result[i][5],
                    })
                else:
                    heroes[len(heroes) - 1]['top_purchases'] = items
                    items = []
                    items.append({
                        name_of_columns[3]: result[i][3],
                        name_of_columns[4]: result[i][4],
                        name_of_columns[5]: result[i][5],
                    })
            elif result[it][1] == result[i][1]:
                items.append({
                    name_of_columns[3]: result[i][3],
                    'id': result[i][4],
                    'name': result[i][5],
                })
            else:
                it = i
                break
        heroes[len(heroes) - 1]['top_purchase'] = items

    item['heroes'] = heroes

    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


def endpoint2(request, ability_id):

    query = (f"""select *
                from (
                        select distinct *, dense_rank() over(partition by res2.hero_id, res2.winner order by res2.cnt DESC) as "rank"
                        from (
                                select *, count(*) over(partition by res.hero_id, res.winner, res.timee) as cnt
                                from (
                                        select ab.id, ab.name, h.id as hero_id, h.localized_name,
                                            CASE
                                                WHEN mpd.player_slot IN (128,129,130,131,132) THEN NOT mt.radiant_win
                                                ELSE mt.radiant_win
                                            END AS winner,
                                            CASE
                                                WHEN cast(au.time as decimal)/mt.duration < 0.1 THEN '0-9'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.2 THEN '10-19'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.3 THEN '20-29'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.4 THEN '30-39'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.5 THEN '40-49'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.6 THEN '50-59'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.7 THEN '60-69'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.8 THEN '70-79'
                                                WHEN cast(au.time as decimal)/mt.duration < 0.9 THEN '80-89'
                                                WHEN cast(au.time as decimal)/mt.duration < 1.0 THEN '90-99'
                                                ELSE '100-109'
                                            END as timee
                                        from abilities as ab
                                        join ability_upgrades as au
                                            on ab.id = au.ability_id
                                        join matches_players_details as mpd
                                            on mpd.id = au.match_player_detail_id
                                        join matches as mt
                                            on mpd.match_id = mt.id
                                        join heroes as h
                                            on h.id = mpd.hero_id
                                        where ab.id = %s
                                    ) res
                                order by cnt desc
                            ) res2
	                ) res3
                where "rank" = 1
                order by res3.hero_id""" % ability_id)

    pass


def endpoint3(request):
    pass
