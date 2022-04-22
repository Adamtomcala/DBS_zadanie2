from django.urls import path

from . import views

urlpatterns = [
    path('matches/<int:match_id>/top_purchases/', views.top_purchases),
    path('abilities/<int:ability_id>/usage/', views.ability_usage),
    path('statistics/tower_kills/', views.tower_kills),
]