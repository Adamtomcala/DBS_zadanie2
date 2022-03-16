from django.urls import path

from . import views

urlpatterns = [
    path('patches', views.endpoint1),
    path('players/<int:player_id>/game_exp', views.endpoint2),
    path('players/<int:player_id>/game_objectives', views.endpoint3),
    path('players/<int:player_id>/abilities', views.endpoint4),
]