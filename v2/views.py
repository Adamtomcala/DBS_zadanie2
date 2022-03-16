import psycopg2
import os
from rest_framework.decorators import api_view
from django.http import JsonResponse


# Create your views here.


@api_view(['GET'])
def endpoint1(request):

    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(f""" SELECT res.patch_version, res.patch_start_date, res.patch_end_date, mt.id as match_id, 
                            ROUND(mt.duration/60.00,2) as match_duration
                            FROM matches as mt
                            right JOIN 
                            (	SELECT 	"name" as patch_version, cast(extract(EPOCH from release_date) as integer)
                                        as patch_start_date,
                                        LEAD(cast(extract(EPOCH from release_date) as integer), 1) OVER(ORDER BY release_date) 
                                        as patch_end_date
                            from patches
                            order by "id") as res
                            ON mt.start_time between res.patch_start_date and res.patch_end_date 
                            ORDER BY res.patch_version, match_id
                            """)

    result = cursor.fetchall()
    # dorobit
    if not result:
        pass

    records = []

    names_of_columns = [desc[0] for desc in cursor.description]
    size = len(result)

    flag = True
    it = 0
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
                    names_of_columns[4]: result[i][4],
                })
            else:
                it = i
                break
        item['matches'] = matches
        records.append(item)

    return JsonResponse({'patches': records}, json_dumps_params={'indent': 3}, status=200)


@api_view(['GET'])
def endpoint2(request, player_id):
    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(f"""SELECT pl.id as "id", COALESCE(pl.nick, 'unknown') as player_nick,
					mt.id as match_id,
					h.localized_name as hero_localized_name,
	                round(mt.duration/60.0, 2) as match_duration_minutes,
	                (coalesce(mpd.xp_hero, 0) + coalesce(mpd.xp_creep, 0) + coalesce(mpd.xp_roshan, 0) + coalesce(mpd.xp_other,0)) as experiences_gained,
	                mpd.level as level_gained,
                    CASE
                            WHEN player_slot IN (128,129,130,131,132) THEN NOT mt.radiant_win
                            ELSE mt.radiant_win
                    END as winner
                    FROM matches_players_details as mpd
                    JOIN players as pl
                        ON mpd.player_id = pl.id
                    JOIN matches as mt
                        ON mpd.match_id = mt.id
                    JOIN heroes as h
                        ON mpd.hero_id = h.id
                    WHERE mpd.player_id = """ + str(player_id))

    result = cursor.fetchall()
    names_of_columns = [desc[0] for desc in cursor.description]

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
            names_of_columns[4]: row[4],
            names_of_columns[5]: row[5],
            names_of_columns[6]: row[6],
            names_of_columns[7]: row[7],
        })

    item['matches'] = matches
    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


@api_view(['GET'])
def endpoint3(request, player_id):
    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT')
    )

    cursor = connection.cursor()

    cursor.execute(f"""select pl.id, pl.nick as player_nick, final_result.match_id,
                       h.localized_name as hero_localized_name,
                       final_result.hero_action, final_result.count
                       from matches_players_details as mpd
                       join players as pl
                            on pl.id = mpd.player_id
                       join matches as mt
                            on mt.id = mpd.match_id
                       join heroes as h
                            on mpd.hero_id = h.id
                       join (
                            select res.player_id, res.match_id, COALESCE(gos.subtype, 'NO_ACTION') as hero_action, 
                                CASE
                                    WHEN gos.subtype is NULL THEN 0
                                    ELSE count(*)
                                END as count
                            from game_objectives as gos
                            right join	(select mpd.id, mpd.match_id, mpd.player_id
                                     from matches_players_details as mpd
                                     where mpd.player_id = %s 
                                     group by mpd.id, mpd.match_id, mpd.player_id
                                     ) as res
                            on gos.match_player_detail_id_1 = res.id
                            group by res.player_id, res.match_id, gos.subtype
                        ) as final_result
                            on final_result.player_id = pl.id and final_result.match_id = mt.id
                        order by mpd.match_id ASC""" % player_id)

    result = cursor.fetchall()

    names_of_columns = [desc[0] for desc in cursor.description]

    if not result:
        pass

    item = {
        names_of_columns[0]: result[0][0],
        names_of_columns[1]: result[0][1],
    }
    matches = []
    size = len(result)
    it = 0

    while it < size:
        matches.append({
            names_of_columns[2]: result[it][2],
            names_of_columns[3]: result[it][3],
        })
        actions = []
        for i in range(it, size):
            if i == size - 1:
                actions.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5]
                })
                it = i + 1
                break
            if result[it][2] == result[i][2]:
                actions.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5]
                })
            else:
                actions.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5]
                })
                it = i
                break
        matches[len(matches) - 1]['actions'] = actions

    item['matches'] = matches

    return JsonResponse(item, json_dumps_params={'indent': 3}, status=200)


@api_view(['GET'])
def endpoint4(request, player_id):
    connection = psycopg2.connect(
        host=os.getenv('DBHOST'),
        database=os.getenv('DBNAME'),
        user=os.getenv('DBUSER'),
        password=os.getenv('DBPASS'),
        port=os.getenv('DBPORT'),
    )

    cursor = connection.cursor()

    cursor.execute(f""" SELECT distinct mp.player_id as "id", pl.nick as player_nick,
                            mp.match_id, h.localized_name as hero_localized_name, 
                            ab.name as ability_name, count(mp.player_id) over(partition by mp.player_id, mp.match_id, au.ability_id),
                            max(au.level) over(partition by mp.player_id, mp.match_id, au.ability_id) as upgrade_level
                            from matches_players_details as mp 
                            join ability_upgrades as au
                                on mp.id = au.match_player_detail_id
                            join abilities as ab
                                on ab.id = au.ability_id
                            join players as pl
                                on mp.player_id = pl.id
                            join heroes as h
                                on mp.hero_id = h.id
                            where mp.player_id = """ + str(player_id))

    result = cursor.fetchall()

    names_of_columns = [desc[0] for desc in cursor.description]

    if not result:
        pass

    item = {
        names_of_columns[0]: result[0][0],
        names_of_columns[1]: result[0][1],
    }

    matches = []
    size = len(result)
    it = 0

    while it < size:
        matches.append({
            names_of_columns[2]: result[it][2],
            names_of_columns[3]: result[it][3],
        })
        abilities = []
        for i in range(it, size):
            if i == size - 1:
                abilities.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5],
                    names_of_columns[6]: result[it][6],
                })
                it = i + 1
                break
            if result[it][2] == result[i][2]:
                abilities.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5],
                    names_of_columns[6]: result[i][6],
                })
            else:
                abilities.append({
                    names_of_columns[4]: result[i][4],
                    names_of_columns[5]: result[i][5],
                    names_of_columns[6]: result[i][6],
                })
                it = i
                break
        matches[len(matches) - 1]['abilities'] = abilities

    item['matches'] = matches

    return JsonResponse(item, json_dumps_params={'indent': 3},status=200)
