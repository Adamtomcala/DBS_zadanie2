from django.urls import path

from . import views

urlpatterns = [
    path('matches/<int:match_id>/top_purchases/', views.endpoint1),
    #path('abilities/<int:ability_id>/usage/', views.endpoint2),
    #path('statistics/tower_kills/', views.endpoint3),
]