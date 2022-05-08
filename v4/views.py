from django.db.models.functions import Rank, Coalesce, Round, Cast, Extract, Lead
from . import models
from django.db.models import Case, When, Value, F, Count, Max, Q, TextField, IntegerField, Sum, FilteredRelation
from django.db.models.expressions import Window, Subquery, OuterRef

from django.http import JsonResponse


def matches_and_patches(request):
    patches = models.Patches.objects.using('dota').raw("""SELECT mt.id, res.patch_version, res.patch_start_date, res.patch_end_date, mt.id AS match_id, 
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
                            ORDER BY res.patch_version, match_id""")

    records = []
    size = len(patches)

    # Pomocne premenne pri formatovani
    flag = True
    it = 0

    # Uprava formatu
    while flag:
        item = {
            'patch_version': patches[it].patch_version,
            'patch_start_date': patches[it].patch_start_date,
            'patch_end_date': patches[it].patch_end_date
        }
        matches = []
        for i in range(it, size):
            if patches[i].patch_version == patches[it].patch_version:
                if patches[i].match_id is None or patches[i].duration is None:
                    print("hajo")
                    if i == size - 1:
                        flag = False
                    else:
                        it = i + 1
                    break
                matches.append({
                    'match_id': patches[i].match_id,
                    'duration': float(patches[i].duration),
                })
            else:
                it = i
                break
        item['matches'] = matches
        records.append(item)

    return JsonResponse({'patches': records}, json_dumps_params={'indent': 3}, status=200)


def game_experiences(request, player_id):
    players = models.MatchesPlayersDetails.objects.using('dota').filter(player=player_id).annotate(
            match_duration=Round(F('match__duration')/60.0, 2),
            player_nick=Coalesce('player__nick', Value('unknown'), output_field=TextField()),
            experiences_gained=Sum(Coalesce('xp_hero', 0, output_field=IntegerField()) +
                                   Coalesce('xp_creep', 0, output_field=IntegerField()) +
                                   Coalesce('xp_roshan', 0, output_field=IntegerField()) +
                                   Coalesce('xp_other', 0, output_field=IntegerField())
                                   ),
            winner=Case(
                When((Q(player_slot__exact=128) | Q(player_slot__exact=129) | Q(player_slot__exact=130) |
                     Q(player_slot__exact=131) |Q(player_slot__exact=132)) & Q(match__radiant_win__exact=True),
                     then=Value(False)),
                When((Q(player_slot__exact=128) | Q(player_slot__exact=129) | Q(player_slot__exact=130) |
                     Q(player_slot__exact=131) | Q(player_slot__exact=132)) & Q(match__radiant_win__exact=False),
                     then=Value(True)),
                When((Q(player_slot__exact=0) | Q(player_slot__exact=1) | Q(player_slot__exact=2) |
                     Q(player_slot__exact=3) | Q(player_slot__exact=4)) & Q(match__radiant_win__exact=True),
                     then=Value(True)),
                When((Q(player_slot__exact=0) | Q(player_slot__exact=1) | Q(player_slot__exact=2) |
                     Q(player_slot__exact=3) | Q(player_slot__exact=4)) & Q(match__radiant_win__exact=False),
                     then=Value(False))
            ))

    if not players:
        pass

    item = {
        'id': players[0].player.id,
        'player_nick': players[0].player.nick,
    }
    matches = []
    for player in players:
        matches.append({
            'match_id': player.match.id,
            'hero_localized_name': player.hero.localized_name,
            'match_duration_minutes': player.match_duration,
            'experience_gained': player.experiences_gained,
            'level_gained': player.level,
            'winner': player.winner,
        })

    item['matches'] = matches
    return JsonResponse(item, status=200, safe=False, json_dumps_params={'indent': 3})


def player_game_objectives(request, player_id):
    game_objectives = models.MatchesPlayersDetails.objects.using('dota').filter(player=player_id).prefetch_related(
        "mpd1").annotate(player_nick=Coalesce('player__nick', Value('unknown'), output_field=TextField()),
                         hero_action=Coalesce('mpd1__subtype', Value('NO_ACTION'), output_field=TextField())
    ).annotate(count=Window(expression=Count('*'), partition_by=[F('id'), F('match'), F('player'), F('hero_action')])).distinct()

    if not game_objectives:
        pass

    item = {
        'id': game_objectives[0].player.id,
        'player_nick': game_objectives[0].player.nick,
    }
    matches = []
    size = len(game_objectives)

    it = 0
    flag = True
    while flag:
        matches.append({
            'match_id': game_objectives[it].match.id,
            'hero_localized_name': game_objectives[it].hero.localized_name,
        })
        actions = []
        for i in range(it, size):
            if i == size - 1:
                flag = False
                if game_objectives[it].match.id == game_objectives[i].match.id:
                    actions.append({
                        'hero_action': game_objectives[i].hero_action,
                        'count': game_objectives[i].count
                    })
                else:
                    matches[len(matches) - 1]['actions'] = actions
                    matches.append({
                        'match_id': game_objectives[i].match.id,
                        'hero_localized_name': game_objectives[i].hero.localized_name,
                    })
                    actions = []
                    actions.append({
                        'hero_action': game_objectives[i].hero_action,
                        'count': game_objectives[i].count
                    })
                break
            if game_objectives[it].match.id == game_objectives[i].match.id:
                actions.append({
                    'hero_action': game_objectives[i].hero_action,
                    'count': game_objectives[i].count
                })
            else:
                it = i
                break

        matches[len(matches) - 1]['actions'] = actions

    item['matches'] = matches

    return JsonResponse(item, status=200, safe=False, json_dumps_params={'indent': 3})


def player_abilities(request, player_id):
    players = models.AbilityUpgrades.objects.using('dota').filter(match_player_detail__player=player_id).annotate(
            count=Window(expression=Count('match_player_detail__player__id'),
                         partition_by=[F('match_player_detail__player__id'), F('match_player_detail__match__id'),
                                       F('ability_id')]),
            upgrade_level=Window(expression=Max('level'),
                                 partition_by=[F('match_player_detail__player__id'), F('match_player_detail__match__id'),
                                       F('ability_id')])
            ).distinct('match_player_detail__player__id', 'match_player_detail__match__id', 'ability_id')

    if not players:
        pass

    size = len(players)
    item = {
        'id': players[0].match_player_detail.player.id,
        'player_nick': players[0].match_player_detail.player.nick,

    }

    matches = []

    it = 0
    flag = True
    while flag:
        matches.append({
            'match_id': players[it].match_player_detail.match.id,
            'hero_localized_name': players[it].match_player_detail.hero.localized_name,
        })
        abilities = []
        for i in range(it, size):
            if i == size - 1:
                flag = False
                if players[it].match_player_detail.match.id == players[i].match_player_detail.match.id:
                    abilities.append({
                        'ability_name': players[i].ability.name,
                        'count': players[i].count,
                        'upgrade_level': players[i].upgrade_level,
                    })
                else:
                    matches[len(matches) - 1]['actions'] = abilities
                    abilities.append({
                        'ability_name': players[i].ability.name,
                        'count': players[i].count,
                        'upgrade_level': players[i].upgrade_level,
                    })
                    abilities = []
                    abilities.append({
                        'ability_name': players[i].ability.name,
                        'count': players[i].count,
                        'upgrade_level': players[i].upgrade_level,
                    })
                break
            if players[it].match_player_detail.match.id == players[i].match_player_detail.match.id:
                abilities.append({
                    'ability_name': players[i].ability.name,
                    'count': players[i].count,
                    'upgrade_level': players[i].upgrade_level,
                })
            else:
                it = i
                break
        matches[len(matches) - 1]['abilities'] = abilities

    item['matches'] = matches

    return JsonResponse(item, status=200, safe=False, json_dumps_params={'indent': 3})


def top_purchases(request, match_id):
    pass


def ability_usage(request, ability_id):
    """
    mpd = models.AbilityUpgrades.objects.using('dota').filter(ability_id=ability_id).annotate(
        winner=Case(
            When((Q(match_player_detail__player_slot__exact=128) | Q(match_player_detail__player_slot__exact=129) | Q(match_player_detail__player_slot__exact=130) |
                  Q(match_player_detail__player_slot__exact=131) | Q(match_player_detail__player_slot__exact=132)) & Q(match_player_detail__match__radiant_win__exact=True),
                 then=Value(False)),
            When((Q(match_player_detail__player_slot__exact=128) | Q(match_player_detail__player_slot__exact=129) | Q(match_player_detail__player_slot__exact=130) |
                  Q(match_player_detail__player_slot__exact=131) | Q(match_player_detail__player_slot__exact=132)) & Q(match_player_detail__match__radiant_win__exact=False),
                 then=Value(True)),
            When((Q(match_player_detail__player_slot__exact=0) | Q(match_player_detail__player_slot__exact=1) | Q(match_player_detail__player_slot__exact=2) |
                  Q(match_player_detail__player_slot__exact=3) | Q(match_player_detail__player_slot__exact=4)) & Q(match_player_detail__match__radiant_win__exact=True),
                 then=Value(True)),
            When((Q(match_player_detail__player_slot__exact=0) | Q(match_player_detail__player_slot__exact=1) | Q(match_player_detail__player_slot__exact=2) |
                  Q(match_player_detail__player_slot__exact=3) | Q(match_player_detail__player_slot__exact=4)) & Q(match_player_detail__match__radiant_win__exact=False),
                 then=Value(False))
        ),
        timee=Case(
            When(Q(F())),
            When(),
            When(),
            When()
            )
    )
    """

def tower_kills(request):
    pass
