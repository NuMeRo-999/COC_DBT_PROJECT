import requests
import snowflake.connector
import json
from datetime import datetime, timedelta
import random
import os
import hashlib
from dotenv import load_dotenv

# ----------------------------
# CARGAR CONFIGURACIÓN DESDE .env
# ----------------------------
load_dotenv()

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

def generate_md5(text):
    """Genera hash MD5"""
    return hashlib.md5(text.encode()).hexdigest()

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
        if player.get("warPreference") == "in":
            playerIn.append({
                "player_tag": player["tag"],
                "name": player["name"]
            })
        elif player.get("warPreference") == "out":
            playerOut.append({
                "player_tag": player["tag"], 
                "name": player["name"]
            })
  
    return {"in": playerIn, "out": playerOut}

# ----------------------------
# War Data Functions
# ----------------------------
def get_current_war_data(clan_tag):
    """Obtiene datos de la guerra actual"""
    try:
        encoded_tag = clan_tag.replace("#", "%23")
        url = f"{BASE_URL}/clans/{encoded_tag}/currentwar"
        data = get_coc_api(url)
        print(f"Current war state: {data.get('state')}")
        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print("No current war found")
            return {'state': 'notInWar'}
        else:
            raise e
    except Exception as e:
        print(f"Error getting current war: {e}")
        return {'state': 'notInWar'}

def get_war_log_data(clan_tag):
    """Obtiene el historial de guerras"""
    try:
        encoded_tag = clan_tag.replace("#", "%23")
        url = f"{BASE_URL}/clans/{encoded_tag}/warlog"
        data = get_coc_api(url)
        print(f"War log items found: {len(data.get('items', []))}")
        return data
    except Exception as e:
        print(f"Error getting war log: {e}")
        return {'items': []}

def generate_war_id(war_data):
    clan_tag = CLAN_TAG
    if 'endTime' in war_data:
        return f"{clan_tag}-{war_data['endTime']}"
    elif 'startTime' in war_data:
        return f"{clan_tag}-{war_data['startTime']}"
    else:
        return f"{clan_tag}-current"

def calculate_war_start_time(end_time):
    """Calcula el start_time basado en end_time (las guerras duran 24h)"""
    try:
        # Convertir el formato de timestamp de la API
        end_str = end_time.replace('T', ' ').replace('.000Z', '')
        end_dt = datetime.strptime(end_str, '%Y%m%d %H%M%S')
        start_dt = end_dt - timedelta(hours=24)
        return start_dt.strftime('%Y%m%dT%H%M%S.000Z')
    except:
        # Fallback si hay error en el parsing
        return (datetime.utcnow() - timedelta(hours=24)).strftime('%Y%m%dT%H%M%S.000Z')

def get_war_context(clan_tag):
    """Obtiene el contexto de guerra actual o más reciente"""
    # Primero intentar con guerra actual
    current_war_data = get_current_war_data(clan_tag)
    
    if current_war_data.get('state') in ['preparation', 'inWar']:
        print("Using current war data")
        return {
            'war_id': generate_war_id(current_war_data),
            'start_time': current_war_data.get('startTime'),
            'end_time': current_war_data.get('endTime'),
            'is_current': True,
            'team_size': current_war_data.get('teamSize', 30)
        }
    
    # Si no hay guerra actual, usar la última guerra del log
    war_log_data = get_war_log_data(clan_tag)
    war_items = war_log_data.get('items', [])
    
    if war_items:
        latest_war = war_items[0]  # La guerra más reciente
        print(f"Using latest war from log: {latest_war.get('endTime')}")
        return {
            'war_id': generate_war_id(latest_war),
            'start_time': calculate_war_start_time(latest_war['endTime']),
            'end_time': latest_war['endTime'],
            'is_current': False,
            'team_size': latest_war.get('teamSize', 30)
        }
    
    # Si no hay guerras, crear una simulación
    print("No war data found, creating simulated war context")
    simulated_end = datetime.utcnow().strftime('%Y%m%dT%H%M%S.000Z')
    simulated_start = (datetime.utcnow() - timedelta(hours=24)).strftime('%Y%m%dT%H%M%S.000Z')
    
    return {
        'war_id': generate_md5(f"{CLAN_TAG}_simulated_{simulated_end}"),
        'start_time': simulated_start,
        'end_time': simulated_end,
        'is_current': False,
        'team_size': 30
    }

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

def generate_map_positions(players, team_size=30):
    """
    Asigna una posición de mapa única por jugador (1–team_size).
    """
    available_positions = list(range(1, team_size + 1))
    random.shuffle(available_positions)

    pos_map = {}
    for i, p in enumerate(players):
        if i < len(available_positions):
            pos_map[p["player_tag"]] = available_positions[i]
        else:
            pos_map[p["player_tag"]] = i + 1

    return pos_map

def random_defender_tag():
    """Genera un defenderTag con formato # + 9 chars (A-Z0-9)."""
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "#" + "".join(random.choice(chars) for _ in range(9))

def generate_attacks(players, war_context=None):
    """
    players = [{ player_tag, name }]
    war_context = { war_id, start_time, end_time, is_current, team_size }
    """
    player_tags = [p["player_tag"] for p in players]
    
    # Usar team_size del war_context o default 30
    team_size = war_context.get('team_size', 30) if war_context else 30
    attack_counts = assign_attack_count(len(player_tags))
    map_positions = generate_map_positions(players, team_size)

    attacks = []

    for i, p in enumerate(players):
        attacker_tag = p["player_tag"]
        num_attacks = attack_counts[i]

        # Posición fija asignada a este jugador
        fixed_map_position = map_positions[attacker_tag]

        for attack_number in range(1, num_attacks + 1):
            defender_tag = random_defender_tag()
            while defender_tag == attacker_tag:  # evitar auto-ataque
                defender_tag = random_defender_tag()

            stars, destruction = generate_stars_and_destruction()

            attack = {
                "attackerTag": attacker_tag,
                "defenderTag": defender_tag,
                "stars": stars,
                "destructionPercentage": destruction,
                "attackNumber": attack_number,
                "mapPosition": fixed_map_position,
                "duration": random.randint(90, 180),
                "ingest_ts": datetime.utcnow().isoformat() + "Z"
            }

            # AÑADIR INFORMACIÓN DE GUERRA SI ESTÁ DISPONIBLE
            if war_context:
                attack["warId"] = war_context["war_id"]
                attack["warStartTime"] = war_context["start_time"]
                attack["warEndTime"] = war_context["end_time"]
                attack["isCurrentWar"] = war_context["is_current"]
                attack["teamSize"] = war_context["team_size"]

            attacks.append(attack)

    return attacks

def generate_and_ingest_attacks():
    conn = get_sf_connection()
    cs = conn.cursor()

    try:
        # 1. Obtener contexto de guerra
        war_context = get_war_context(CLAN_TAG)
        
        # 2. Get players
        members = get_players_with_war_preference(CLAN_TAG)
        players = []
        
        # Limitar players según el team_size de la guerra
        team_size = war_context.get('team_size', 30)
        
        if len(members["in"]) >= team_size:
            players = members["in"][:team_size]
        elif len(members["in"]) < team_size:
            players = members["in"] + members["out"]
            players = players[:team_size]
        
        print(f"Total players available for war: {len(players)}")
        print(f"War context: {war_context['war_id']}")
        print(f"Team size: {team_size}")

        if not players:
            print("No players available for war attacks")
            return

        # 3. Generate attacks with war context
        attacks = generate_attacks(players, war_context)

        for atk in attacks:
            tag = atk["attackerTag"]

            cs.execute("""
                INSERT INTO attack_raw (player_tag, raw)
                SELECT %s, PARSE_JSON(%s)
            """, (tag, json.dumps(atk)))

        conn.commit()
        print(f"{len(attacks)} ataques generados e insertados en Snowflake")
        print(f"War ID used: {war_context['war_id']}")

    except Exception as e:
        print("Error insertando ataques:", e)
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cs.close()
        conn.close()

# =====================================================================
# FUNCIONES DE INGESTA ORIGINALES (mantenidas para compatibilidad)
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
    # generate_and_ingest_attacks()
    ingest_players()