from django.db.models.functions import Rank, Coalesce, Round, Cast, Extract, Lead, RowNumber
from . import models
from django.db.models import Case, When, Value, F, Count, Max, Q, TextField, IntegerField, Sum, FilteredRelation
from django.db.models.expressions import Window, Subquery, OuterRef, ExpressionWrapper

from django.http import JsonResponse


def matches_and_patches(request):
    pass


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
    mpd = models.MatchesPlayersDetails.objects.using('dota').filter(
            Q(
                ((Q(player_slot__exact=128) | Q(player_slot__exact=129) | Q(player_slot__exact=130) |
                  Q(player_slot__exact=131) | Q(player_slot__exact=132)) & Q(match__radiant_win__exact=False)) |
                ((Q(player_slot__exact=0) | Q(player_slot__exact=1) | Q(player_slot__exact=2) |
                  Q(player_slot__exact=3) | Q(player_slot__exact=4)) & Q(match__radiant_win__exact=True))
             ), match=match_id).prefetch_related("log").annotate(count=Window(expression=Count('*'),
                         partition_by=[F('log__match_player_detail'), F('log__item')]), log_id=F('log')).distinct('match', 'hero',
                                                'hero__localized_name', 'log__item', 'log__item__name', 'count').order_by(
        'hero', '-count', 'log__item').annotate(
        rank=Window(expression=RowNumber()
                    )
    )

    if not mpd:
        pass

    item = {
        'id': mpd[0].match.id,
    }

    size = len(mpd)
    flag = True
    heroes = []
    it = 0
    while flag:
        is_hero = True
        heroes.append({
            'id': mpd[it].hero.id,
            'name': mpd[it].hero.localized_name,
        })
        purchase = []
        cnt = 0
        for i in range(it, size):
            if i == size - 1:
                flag = False
            if cnt < 5 and mpd[it].hero.id == mpd[i].hero.id:
                pl = mpd[i].log.get(id=mpd[i].log_id)
                purchase.append({
                    'count': mpd[i].count,
                    'id': pl.item.id,
                    'name': pl.item.name
                })
                cnt += 1
            elif mpd[it].hero.id != mpd[i].hero.id:
                it = i
                break
        heroes[len(heroes) - 1]['top_purchase'] = purchase

    item['heroes'] = heroes

    return JsonResponse(item, status=200, safe=False, json_dumps_params={'indent': 3})


def ability_usage(request, ability_id):
    from django.db.models import FloatField
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
        timee=ExpressionWrapper(F('time')*1.0/F('match_player_detail__match__duration'), output_field=FloatField())
    ).annotate(
        bucket=Case(
            When(Q(timee__lt=0.1), then=Value('0-9')),
            When(Q(timee__lt=0.2), then=Value('10-19')),
            When(Q(timee__lt=0.3), then=Value('20-29')),
            When(Q(timee__lt=0.4), then=Value('30-39')),
            When(Q(timee__lt=0.5), then=Value('40-39')),
            When(Q(timee__lt=0.6), then=Value('50-59')),
            When(Q(timee__lt=0.7), then=Value('60-69')),
            When(Q(timee__lt=0.8), then=Value('70-79')),
            When(Q(timee__lt=0.9), then=Value('80-89')),
            When(Q(timee__lt=1.0), then=Value('90-99')),
            default=Value('100-109')
        )
    ).annotate(
        count=Window(expression=Count('*'), partition_by=[F('ability'), F('ability__name'),
                                          F('match_player_detail__hero'), F('match_player_detail__hero__localized_name'),
                                          F('winner'), F('bucket')])
    ).distinct('ability', 'ability__name', 'match_player_detail__hero', 'match_player_detail__hero__localized_name',
               'winner', 'bucket', 'count').order_by('-match_player_detail__hero', 'winner', 'count').reverse()

    if not mpd:
        pass

    item = {
        'id': mpd[0].ability.id,
        'name': mpd[0].ability.name,
    }
    size = len(mpd)
    heroes = []
    it = 0
    flag = True

    while flag:

        hero = mpd[it].match_player_detail.hero.id
        heroes.append({
            'id': hero,
            'name': mpd[it].match_player_detail.hero.localized_name
        })
        winner = True
        win_dir = {}
        loss = True
        loss_dir = {}
        for i in range(it, size):
            if i == size - 1:
                flag = False
                if hero != mpd[i].match_player_detail.hero.id:
                    if win_dir:
                        heroes[len(heroes) - 1]['usage_winners'] = win_dir
                    if loss_dir:
                        heroes[len(heroes) - 1]['usage_loosers'] = loss_dir

                    hero = mpd[i].match_player_detail.hero.id
                    heroes.append({
                        'id': hero,
                        'name': mpd[i].match_player_detail.hero.localized_name
                    })
                    winner = True
                    win_dir = {}
                    loss = True
                    loss_dir = {}

                    if mpd[i].winner and winner:
                        win_dir['bucket'] = mpd[i].bucket
                        win_dir['count'] = mpd[i].count
                        winner = False
                    elif not mpd[i].winner and loss:
                        loss_dir['bucket'] = mpd[i].bucket
                        loss_dir['count'] = mpd[i].count
                        loss = False

            if hero == mpd[i].match_player_detail.hero.id:
                if mpd[i].winner and winner:
                    win_dir['bucket'] = mpd[i].bucket
                    win_dir['count'] = mpd[i].count
                    winner = False
                elif not mpd[i].winner and loss:
                    loss_dir['bucket'] = mpd[i].bucket
                    loss_dir['count'] = mpd[i].count
                    loss = False
            else:
                it = i
                break
        if win_dir:
            heroes[len(heroes) - 1]['usage_winners'] = win_dir
        if loss_dir:
            heroes[len(heroes) - 1]['usage_loosers'] = loss_dir

    item['heroes'] = heroes

    return JsonResponse(item, status=200, safe=False, json_dumps_params={'indent': 3})


def tower_kills(request):
    pass
