from django.urls import path

from . import views

urlpatterns = [
    path('patches/', views.matches_and_patches),
    path('players/<int:player_id>/game_exp/', views.game_experiences),
    path('players/<int:player_id>/game_objectives/', views.player_game_objectives),
    path('players/<int:player_id>/abilities/', views.player_abilities),
    path('matches/<int:match_id>/top_purchases/', views.top_purchases),
    path('abilities/<int:ability_id>/usage/', views.ability_usage),
    path('statistics/tower_kills/', views.tower_kills),
]