import requests
import snowflake.connector
import json
from datetime import datetime, timedelta
import random
import os
from dotenv import load_dotenv

# ----------------------------
# CARGAR CONFIGURACIÓN DESDE .env
# ----------------------------
load_dotenv()  # Carga las variables del archivo .env

# ----------------------------
# CONFIG (ahora desde variables de entorno)
# ----------------------------
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")

COC_API_KEY = os.getenv("COC_API_KEY")
HEADERS = {"Authorization": f"Bearer {COC_API_KEY}"}

CLAN_TAG = os.getenv("CLAN_TAG")
BASE_URL = "https://api.clashofclans.com/v1"

# ----------------------------
# Snowflake conexión 
# ----------------------------
def get_sf_connection():
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )

# ----------------------------
# Helper: GET API
# ----------------------------
def get_coc_api(url):
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

# ----------------------------
# Clan Members Functions
# ----------------------------
def get_clan_members(clan_tag):
    url = f"{BASE_URL}/clans/{clan_tag.replace('#','%23')}/members"
    data = get_coc_api(url)
    return data["items"]

def get_player(tag):
    url = f"{BASE_URL}/players/{tag.replace('#','%23')}"
    return get_coc_api(url)

def get_players_with_war_preference(clan_tag):
    members = get_clan_members(clan_tag)

    playerIn = [] 
    playerOut = []

    for m in members:
        player = get_player(m["tag"])
        if player["warPreference"] == "in":
            playerIn.append({
                "player_tag": player["tag"],
                "name": player["name"]
            })
        elif player["warPreference"] == "out":
            playerOut.append({
                "player_tag": player["tag"], 
                "name": player["name"]
            })
  
    return {"in": playerIn, "out": playerOut}

# =====================================================================
# GENERADOR DE ATAQUES REALISTAS
# =====================================================================

def assign_attack_count(num_players):
    """
    80% → 2 ataques
    15% → 1 ataque
    5% → 0 ataques
    """
    counts = []
    for _ in range(num_players):
        r = random.random()
        if r < 0.80:
            counts.append(2)
        elif r < 0.95:
            counts.append(1)
        else:
            counts.append(0)
    return counts

def generate_stars_and_destruction():
    stars = random.choices([0,1,2,3], weights=[0.10,0.30,0.40,0.20])[0]

    if stars == 3:
        destruction = 100
    elif stars == 2:
        destruction = random.randint(50, 99)
    elif stars == 1:
        destruction = random.randint(1, 99)
    else:
        destruction = random.randint(0, 49)

    return stars, destruction

def generate_map_positions(players):
    """
    Asigna una posición de mapa única por jugador (1–30).
    """
    available_positions = list(range(1, len(players) + 1))
    random.shuffle(available_positions)

    pos_map = {}
    for i, p in enumerate(players):
        pos_map[p["player_tag"]] = available_positions[i]

    return pos_map

def generate_attacks(players):
    """
    players = [{ player_tag, name }]
    """
    player_tags = [p["player_tag"] for p in players]
    attack_counts = assign_attack_count(len(player_tags))

    # POSICIONES ÚNICAS Y FIJAS POR JUGADOR
    map_positions = generate_map_positions(players)

    attacks = []

    for i, p in enumerate(players):
        attacker_tag = p["player_tag"]
        num_attacks = attack_counts[i]

        # Posición fija asignada a este jugador
        fixed_map_position = map_positions[attacker_tag]

        for attack_number in range(1, num_attacks + 1):
            defender_tag = random.choice(player_tags)
            while defender_tag == attacker_tag:  # evitar auto-ataque
                defender_tag = random.choice(player_tags)

            stars, destruction = generate_stars_and_destruction()

            attack = {
                "attackerTag": attacker_tag,
                "defenderTag": defender_tag,
                "stars": stars,
                "destructionPercentage": destruction,
                "attackNumber": attack_number,
                "mapPosition": fixed_map_position,  # ← POSICIÓN FIJA
                "duration": random.randint(90, 180),
                "ingest_ts": datetime.utcnow().isoformat() + "Z"
            }

            attacks.append(attack)

    return attacks

def generate_and_ingest_attacks():
    conn = get_sf_connection()
    cs = conn.cursor()

    try:
        # Get players with proper structure
        members = get_players_with_war_preference(CLAN_TAG)
        players = []

        if len(members["in"]) >= 30:
            # limitar el array a máximo 30 jugadores
            players = members["in"]
        elif len(members["in"]) < 30:
            # agregar jugadores con preferencia de guerra out
            players = members["in"] + members["out"]
        
        # limito el array a 30 jugadores
        players = players[:30]
        
        print(f"Total players available for war: {len(players)}")
        print(f"Players IN: {len(members['in'])}, Players OUT: {len(members['out'])}")

        if not players:
            print("No players available for war attacks")
            return

        attacks = generate_attacks(players)

        for atk in attacks:
            tag = atk["attackerTag"]

            cs.execute("""
                INSERT INTO attack_raw (player_tag, raw)
                SELECT %s, PARSE_JSON(%s)
            """, (tag, json.dumps(atk)))

        conn.commit()
        print(f"{len(attacks)} ataques generados e insertados en Snowflake")

    except Exception as e:
        print("Error insertando ataques:", e)
        import traceback
        traceback.print_exc()
        conn.rollback()

    finally:
        cs.close()
        conn.close()

# =====================================================================
# FUNCIONES DE INGESTA ORIGINALES
# =====================================================================

def ingest_clan(clan_tag):
    encoded_tag = clan_tag.replace("#", "%23")
    url = f"{BASE_URL}/clans/{encoded_tag}"
    data = get_coc_api(url)

    conn = get_sf_connection()
    cs = conn.cursor()
    try:
        cs.execute("""
            INSERT INTO clan_raw (clan_tag, raw)
            SELECT %s, PARSE_JSON(%s)
        """, (clan_tag, json.dumps(data)))
        conn.commit()
    finally:
        cs.close()
        conn.close()

def ingest_currentwar(clan_tag):
    encoded_tag = clan_tag.replace("#", "%23")
    url = f"{BASE_URL}/clans/{encoded_tag}/currentwar"
    data = get_coc_api(url)

    conn = get_sf_connection()
    cs = conn.cursor()
    try:
        cs.execute("""
            INSERT INTO currentwar_raw (clan_tag, raw)
            SELECT %s, PARSE_JSON(%s)
        """, (clan_tag, json.dumps(data)))
        conn.commit()
    finally:
        cs.close()
        conn.close()

def ingest_warlog(clan_tag):
    encoded_tag = clan_tag.replace("#", "%23")
    url = f"{BASE_URL}/clans/{encoded_tag}/warlog"
    data = get_coc_api(url)

    conn = get_sf_connection()
    cs = conn.cursor()

    try:
        for war in data.get("items", []):
            cs.execute("""
                INSERT INTO warlog_raw (clan_tag, raw)
                SELECT %s, PARSE_JSON(%s)
            """, (clan_tag, json.dumps(war)))

        conn.commit()

    finally:
        cs.close()
        conn.close()

def ingest_players():
    members = get_clan_members(CLAN_TAG)

    conn = get_sf_connection()
    cs = conn.cursor()
    players_for_attacks = []

    try:
        for m in members:
            tag = m["tag"]
            print("Descargando e insertando:", tag)

            player = get_player(tag)

            cs.execute("""
                INSERT INTO player_raw (player_tag, raw)
                SELECT %s, PARSE_JSON(%s)
            """, (tag, json.dumps(player)))

            players_for_attacks.append({
                "player_tag": tag,
                "name": player["name"]
            })

        conn.commit()

    except Exception as e:
        print("Error:", e)
        conn.rollback()

    finally:
        cs.close()
        conn.close()

    return players_for_attacks

# =====================================================================
# EJECUCIÓN
# =====================================================================

if __name__ == "__main__":
    generate_and_ingest_attacks()