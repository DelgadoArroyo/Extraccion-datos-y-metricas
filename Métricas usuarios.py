import tweepy
import pandas as pd
from authentication import authentication
from datetime import datetime
from decimal import *
auth = authentication()
consumer_key = auth.getconsumer_key()
consumer_secret = auth.getconsumer_secret()
access_token = auth.getaccess_token()
access_token_secret = auth.getaccess_token_secret()
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.secure = True
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=10, retry_delay=5,
                 retry_errors=5)

cuentas = ["populares"]
twts = pd.read_json("populares2.json")
tweets = twts.T

rts = []
favs = []
quotes = 0

for rt in tweets['retweet_count']:
    if rt != 0:
        rts.append(rt)
for fav in tweets['favorite_count']:
    if fav != 0:
        favs.append(fav)
for quote in tweets['is_quote_status']:
    if quote:
        quotes = quotes + 1

publicaciones_originales = tweets.shape[0]
likes_recibidos = sum(favs)
retweets_recibidos = sum(rts)
now = datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

for nombre in cuentas:
    usuario = api.get_user(nombre)

id = usuario.id
nacimiento_cuenta = usuario.created_at
tiempo = now - nacimiento_cuenta
tiempo2 = tiempo.days
followers = usuario.followers_count
friends = usuario.friends_count
nombre = usuario.name
favoriteados = usuario.favourites_count
verificado = usuario.verified
biografia = usuario.description
localizacion =usuario.location
publicaciones = usuario.statuses_count
ratio_ff = (followers/friends)
ratio_pt = (publicaciones_originales/tiempo2)
ratio_ft = (followers/tiempo2)
perfil = usuario.default_profile
foto = usuario.default_profile_image
Rts_hechos = publicaciones - publicaciones_originales
rts_por_dia = (Rts_hechos/tiempo2)
quotest = quotes/publicaciones_originales

contestaciones = 0
contestacion = tweets['in_reply_to_user_id']
for i in range(len(tweets['in_reply_to_user_id'])):
    if contestacion[i]!= None:
        contestaciones = contestaciones + 1
    else:
        continue
publicaciones_publicas = publicaciones_originales - contestaciones

interacciones = []
favoritos = tweets['favorite_count']
retuiteados = tweets['retweet_count']
for i in range(len(tweets['retweet_count'])):
    interaccion = retuiteados[i] + favoritos[i]
    engagement = (interaccion/followers)*100
    interacciones.append(engagement)

media_interacciones = (sum(interacciones)/(len(interacciones)))
interacciones2 = []
for i in range(len(tweets['retweet_count'])):
    interaccion2 = retuiteados[i] + favoritos[i]
    engagement2 = (interaccion2/followers)*100
    if engagement2 >= (media_interacciones):
        interacciones2.append(engagement2)
    else:
        continue
media_interacciones2 = (sum(interacciones2)/(len(interacciones2)))

#PUNTUACIONES DE VIRALIDAD
metricas = [id, nombre, followers, friends, verificado, nacimiento_cuenta, tiempo2, biografia, localizacion, favoriteados, publicaciones, ratio_ff, ratio_pt, ratio_ft, perfil, foto, rts_por_dia]


Detalles_perfil = 0
if biografia != None:
    Detalles_perfil = Detalles_perfil + 1
if not perfil:
    Detalles_perfil = Detalles_perfil + 1
if not foto:
    Detalles_perfil = Detalles_perfil + 1

if Detalles_perfil < 2:
    penalizacion_Detalles_perfil = -0.05
else:
    penalizacion_Detalles_perfil = +0.05

if ratio_pt < 7:
    if ratio_pt < 4:
        if ratio_pt < 2:
            penalizacion_ratio_pt = - 0.1
        else:
            penalizacion_ratio_pt = - 0.05
    else:
        penalizacion_ratio_pt = + 0.1
else:
    penalizacion_ratio_pt = - 0.05

if ratio_ft < 150:
    if ratio_ft < 100:
        if ratio_ft < 50:
            penalizacion_ratio_ft = - 0.05
        else:
            penalizacion_ratio_ft = + 0.05
    else:
        penalizacion_ratio_ft = + 0.1
else:
    penalizacion_ratio_ft = + 0.15

if ratio_ff < 300:
    if ratio_ff < 200:
        if ratio_ff < 100:
            penalizacion_ratio_ff = - 0.05
        else:
            penalizacion_ratio_ff = +0.05
    else:
        penalizacion_ratio_ff = + 0.1
else:
    penalizacion_ratio_ff = + 0.15

if Rts_hechos/metricas[6] < 12:
    if Rts_hechos/metricas[6] < 8:
        if Rts_hechos/metricas[6] < 5:
            penalizacion_Rts = - 0.05
        else:
            penalizacion_Rts = +0.05
    else:
        penalizacion_Rts = + 0.02
else:
    penalizacion_Rts = - 0.05

if (retweets_recibidos/publicaciones_originales) < 200:
    if (retweets_recibidos/publicaciones_originales) < 120:
        if (retweets_recibidos/publicaciones_originales) < 60:
            penalizacion_Rts2 = - 0.05
        else:
            penalizacion_Rts2 = +0.03
    else:
        penalizacion_Rts2 = + 0.06
else:
    penalizacion_Rts2 = +0.1

if ratio_ff <= 1000:
    ratio_ff = ratio_ff
else:
    ratio_ff = 1000

penalizaciones = [penalizacion_ratio_pt, penalizacion_Detalles_perfil
    , penalizacion_ratio_ft, penalizacion_ratio_ff, penalizacion_Rts, penalizacion_Rts2]
engagement_penalizaciones = media_interacciones2 + sum(penalizaciones) + quotest

ratio_likes_rts = (retweets_recibidos/publicaciones_originales)/\
                  (likes_recibidos/publicaciones_originales)

puntuaciones = [ratio_pt, Rts_hechos/metricas[6], retweets_recibidos/publicaciones_originales
    , likes_recibidos/publicaciones_originales, ratio_ff, ratio_ft]
total = 0
for i in puntuaciones:
    total = total + i

total2 = total * engagement_penalizaciones
total3 = total2 * ratio_likes_rts

print("Numero de publicaciones originales: " + str(publicaciones_originales))
print("Media publicaciones originales por dia: " + str(Decimal(ratio_pt).quantize(Decimal('.01'), rounding=ROUND_DOWN)))
print("Numero de retweets hechos: "+ str(Rts_hechos))
print("Media retweets hechos por dia: " + str(Decimal(Rts_hechos/metricas[6]).quantize(Decimal('.001'), rounding=ROUND_DOWN)))
print("Numero de menciones hechas: "+ str(quotes))
print("Total retweets recibidos:" + str(retweets_recibidos))
print("Media retweets recibidos por tweet: " + str(Decimal(retweets_recibidos/publicaciones_originales).quantize(Decimal('.001'), rounding=ROUND_DOWN)))
print("Total likes recibidos:" + str(likes_recibidos))
print("Media likes recibidos por tweet: " + str(Decimal(likes_recibidos/publicaciones_originales).quantize(Decimal('.001'), rounding=ROUND_DOWN)))
print("Edad cuenta= "+ str(Decimal(metricas[6]/365).quantize(Decimal('.001'), rounding=ROUND_DOWN)) + " años")
print("Ratio seguidores/seguidos= "+ str(Decimal(ratio_ff).quantize(Decimal('.01'), rounding=ROUND_DOWN)))
print("Followers por dia= "+ str(Decimal(ratio_ft).quantize(Decimal('.01'), rounding=ROUND_DOWN)))
print("Media engagement: " + str(Decimal(media_interacciones2).quantize(Decimal('.0001'), rounding=ROUND_DOWN))+ "%")
#print("Media OTRA engagement: " + str(Decimal(((retweets_recibidos/publicaciones_originales) + (likes_recibidos/publicaciones_originales))/publicaciones_originales).quantize(Decimal('.0001'), rounding=ROUND_DOWN)))
print("Media engagement con penalizaciones: " + str(Decimal(engagement_penalizaciones).quantize(Decimal('.0001'), rounding=ROUND_DOWN))+ "%")
#print("Puntuacion sin nada: " + str(total))
#print("Penalizaciones: " + str(sum(penalizaciones)))
#print("Puntación total con penalizaciones: " + str(Decimal(total2).quantize(Decimal('.0001'), rounding=ROUND_DOWN)))
print("Puntación total con ratios: " + str(Decimal(total3).quantize(Decimal('.0001'), rounding=ROUND_DOWN)))