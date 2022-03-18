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


@api_view(['GET'])
def endpoint1(request):

    query = f"""SELECT res.patch_version, res.patch_start_date, res.patch_end_date, mt.id AS match_id, 
                            ROUND(mt.duration/60.00,2) AS "duration"
                            FROM matches AS mt
                            RIGHT JOIN 
                            (	SELECT 	"name" AS patch_version, CAST(EXTRACT (EPOCH FROM release_date) AS INTEGER)
                                        AS patch_start_date,
                                        LEAD(CAST(EXTRACT (EPOCH FROM release_date) AS INTEGER), 1) OVER(ORDER BY release_date) 
                                        AS patch_end_date
                            FROM patches
                            ORDER BY "id") AS res
                                ON mt.start_time BETWEEN res.patch_start_date AND res.patch_end_date 
                            ORDER BY res.patch_version, match_id"""

    result, names_of_columns = get_result_and_columns(query)

    records = []
    size = len(result)

    # Pomocne premenne pri formatovani
    flag = True
    it = 0

    # Uprava formatu
    while flag:
        item = {
            names_of_columns[0]: result[it][0],
            names_of_columns[1]: result[it][1],
            names_of_columns[2]: result[it][2]
        }
        matches = []
        for i in range(it, size):
            if result[i][0] == result[it][0]:
                if result[i][3] is None or result[i][4] is None:
                    if i == size - 1:
                        flag = False
                    else:
                        it = i + 1
                    break
                matches.append({
                    names_of_columns[3]: result[i][3],
                    names_of_columns[4]: float(result[i][4]),
                })
            else:
                it = i
                break
        item['matches'] = matches
        records.append(item)

    return JsonResponse({'patches': records}, json_dumps_params={'indent': 3}, status=200)


@api_view(['GET'])
def endpoint2(request, player_id):

    query = f"""SELECT pl.id AS "id", COALESCE(pl.nick, 'unknown') AS player_nick,
                            mt.id AS match_id,
                            h.localized_name AS hero_localized_name,
                            ROUND(mt.duration/60.0, 2) AS match_duration_minutes,
                            (COALESCE(mpd.xp_hero, 0) + COALESCE(mpd.xp_creep, 0) + COALESCE(mpd.xp_roshan, 0) + COALESCE(mpd.xp_other,0)) AS experiences_gained,
                            mpd.level AS level_gained,
                            CASE
                                    WHEN player_slot IN (128,129,130,131,132) THEN NOT mt.radiant_win
                                    ELSE mt.radiant_win
                            END AS winner
                            FROM matches_players_details AS mpd
                            JOIN players AS pl
                                ON mpd.player_id = pl.id
                            JOIN matches AS mt
                                ON mpd.match_id = mt.id
                            JOIN heroes AS h
                                ON mpd.hero_id = h.id
                            WHERE mpd.player_id =""" + str(player_id) + f"""ORDER BY match_id"""

    result, names_of_columns = get_result_and_columns(query)

    if not result:
        pass

    item = {
        names_of_columns[0]: result[0][0],
        names_of_columns[1]: result[0][1],
    }
    matches = []
    for row in result:
        matches.append({
            names_of_columns[2]: row[2],
            names_of_columns[3]: row[3],
            names_of_columns[4]: float(row[4]),
            names_of_columns[5]: row[5],
            names_of_columns[6]: row[6],
            names_of_columns[7]: row[7],
        })

    item['matches'] = matches
    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


@api_view(['GET'])
def endpoint3(request, player_id):

    query = f"""SELECT pl.id, pl.nick AS player_nick, final_result.match_id,
                       h.localized_name AS hero_localized_name,
                       final_result.hero_action, final_result.count
                       FROM matches_players_details AS mpd
                       JOIN players AS pl
                            ON pl.id = mpd.player_id
                       JOIN matches AS mt
                            ON mt.id = mpd.match_id
                       JOIN heroes AS h
                            ON mpd.hero_id = h.id
                       JOIN (
                            SELECT res.player_id, res.match_id, COALESCE(gos.subtype, 'NO_ACTION') AS hero_action, 
                                CASE
                                    WHEN gos.subtype IS NULL THEN 1
                                    ELSE COUNT(*)
                                END AS count
                            FROM game_objectives AS gos
                            RIGHT JOIN	(SELECT mpd.id, mpd.match_id, mpd.player_id
                                     FROM matches_players_details AS mpd
                                     WHERE mpd.player_id = %s 
                                     GROUP BY mpd.id, mpd.match_id, mpd.player_id
                                     ) AS res
                            ON gos.match_player_detail_id_1 = res.id
                            GROUP BY res.player_id, res.match_id, gos.subtype
                        ) AS final_result
                            ON final_result.player_id = pl.id AND final_result.match_id = mt.id
                        ORDER BY  mpd.match_id ASC""" % player_id

    result, names_of_columns = get_result_and_columns(query)

    if not result:
        pass

    item = {
        names_of_columns[0]: result[0][0],
        names_of_columns[1]: result[0][1],
    }
    matches = []
    size = len(result)

    it = 0
    flag = True
    while flag:
        matches.append({
            names_of_columns[2]: result[it][2],
            names_of_columns[3]: result[it][3],
        })
        actions = []
        for i in range(it, size):
            if i == size - 1:
                flag = False
                if result[it][2] == result[i][2]:
                    actions.append({
                        names_of_columns[4]: result[i][4],
                        names_of_columns[5]: result[i][5]
                    })
                else:
                    matches[len(matches) - 1]['actions'] = actions
                    matches.append({
                        names_of_columns[2]: result[i][2],
                        names_of_columns[3]: result[i][3],
                    })
                    actions = []
                    actions.append({
                        names_of_columns[4]: result[i][4],
                        names_of_columns[5]: result[i][5]
                    })
                break
            if result[it][2] == result[i][2]:
                actions.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5]
                })
            else:
                it = i
                break

        matches[len(matches) - 1]['actions'] = actions

    item['matches'] = matches

    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


@api_view(['GET'])
def endpoint4(request, player_id):

    query = (f"""SELECT DISTINCT mp.player_id AS "id", pl.nick AS player_nick,
                            mp.match_id, h.localized_name AS hero_localized_name, 
                            ab.name AS ability_name, COUNT(mp.player_id) OVER(PARTITION BY mp.player_id, mp.match_id, au.ability_id),
                            MAX(au.level) OVER(PARTITION BY mp.player_id, mp.match_id, au.ability_id) AS upgrade_level
                            FROM matches_players_details AS mp 
                            JOIN ability_upgrades AS au
                                ON mp.id = au.match_player_detail_id
                            JOIN abilities AS ab
                                ON ab.id = au.ability_id
                            JOIN players AS pl
                                ON mp.player_id = pl.id
                            JOIN heroes AS h
                                ON mp.hero_id = h.id
                            WHERE mp.player_id = %s""" % player_id)

    result, names_of_columns = get_result_and_columns(query)

    if not result:
        pass

    item = {
        names_of_columns[0]: result[0][0],
        names_of_columns[1]: result[0][1],
    }

    matches = []
    size = len(result)
    it = 0

    it = 0
    flag = True

    while flag:
        matches.append({
            names_of_columns[2]: result[it][2],
            names_of_columns[3]: result[it][3],
        })
        abilities = []
        for i in range(it, size):
            if i == size - 1:
                flag = False
                if result[it][2] == result[i][2]:
                    abilities.append({
                        names_of_columns[4]: result[i][4],
                        names_of_columns[5]: result[i][5],
                        names_of_columns[6]: result[i][6],
                    })
                else:
                    matches[len(matches) - 1]['actions'] = abilities
                    abilities.append({
                        names_of_columns[4]: result[i][4],
                        names_of_columns[5]: result[i][5],
                        names_of_columns[6]: result[i][6],
                    })
                    abilities = []
                    abilities.append({
                        names_of_columns[4]: result[i][4],
                        names_of_columns[5]: result[i][5],
                        names_of_columns[6]: result[i][6],
                    })
                break
            if result[it][2] == result[i][2]:
                abilities.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5],
                    names_of_columns[6]: result[i][6],
                })
            else:
                it = i
                break
        matches[len(matches) - 1]['abilities'] = abilities

    item['matches'] = matches

    return JsonResponse(item, json_dumps_params={'indent': 3},status=200)
