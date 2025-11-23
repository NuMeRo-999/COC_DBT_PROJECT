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
# CONFIGURACIÓN DE ITERACIONES
# ----------------------------
NUM_ITERATIONS = 3  # Número de iteraciones para generar historiales
OUTPUT_DIR = "player_history"  # Carpeta donde se guardarán los archivos (opcional)

# Timestamp fijo para datos originales
ORIGINAL_TIMESTAMP = datetime(2025, 11, 22, 14, 47, 50, 286000)

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

# ----------------------------
# Funciones de modificación de perfil
# ----------------------------
def modify_trophies(value):
    """Modifica trophies con un cambio aleatorio del 1-5% (puede subir o bajar)"""
    percentage = random.uniform(0.01, 0.05)
    change = int(value * percentage)
    if random.choice([True, False]):
        return value + change
    else:
        return max(0, value - change)

def reduce_value(value):
    """Reduce un valor entre 1-5%"""
    percentage = random.uniform(0.01, 0.05)
    reduction = int(value * percentage)
    return max(0, value - reduction)

def modify_troops(troops):
    """Reduce el nivel de algunas tropas aleatoriamente (máximo 1 nivel)"""
    if not troops:
        return
    num_troops_to_modify = random.randint(2, min(5, len(troops)))
    troops_eligible = [i for i, t in enumerate(troops) if t.get('level', 1) > 1]
    if troops_eligible:
        num_to_select = min(num_troops_to_modify, len(troops_eligible))
        troops_to_modify = random.sample(troops_eligible, num_to_select)
        for idx in troops_to_modify:
            troops[idx]['level'] -= 1

def modify_heroes(heroes):
    """Reduce el nivel de máximo 2 héroes aleatoriamente (máximo 1 nivel cada uno)"""
    if not heroes:
        return
    heroes_eligible = [i for i, h in enumerate(heroes) if h.get('level', 1) > 1]
    if heroes_eligible:
        num_heroes_to_modify = min(2, len(heroes_eligible))
        heroes_to_modify = random.sample(heroes_eligible, num_heroes_to_modify)
        for idx in heroes_to_modify:
            heroes[idx]['level'] -= 1

def modify_hero_equipment(equipment):
    """Reduce el nivel de equipamiento aleatoriamente (1 o 2 niveles)"""
    if not equipment:
        return
    num_equipment_to_modify = random.randint(2, min(5, len(equipment)))
    equipment_eligible = [i for i, e in enumerate(equipment) if e.get('level', 1) > 1]
    if equipment_eligible:
        num_to_select = min(num_equipment_to_modify, len(equipment_eligible))
        equipment_to_modify = random.sample(equipment_eligible, num_to_select)
        for idx in equipment_to_modify:
            reduction = random.choice([1, 2])
            equipment[idx]['level'] = max(1, equipment[idx]['level'] - reduction)

def modify_spells(spells):
    """Reduce el nivel de algunos hechizos aleatoriamente (máximo 1 nivel)"""
    if not spells:
        return
    num_spells_to_modify = random.randint(2, min(5, len(spells)))
    spells_eligible = [i for i, s in enumerate(spells) if s.get('level', 1) > 1]
    if spells_eligible:
        num_to_select = min(num_spells_to_modify, len(spells_eligible))
        spells_to_modify = random.sample(spells_eligible, num_to_select)
        for idx in spells_to_modify:
            spells[idx]['level'] -= 1

def modify_player_profile(player_data):
    """
    Aplica todas las transformaciones a un perfil de jugador.
    Retorna una copia modificada del player_data.
    """
    # Crear una copia profunda para no modificar el original
    modified_data = json.loads(json.dumps(player_data))

    # Modificar estadísticas principales
    if 'trophies' in modified_data:
        modified_data['trophies'] = modify_trophies(modified_data['trophies'])

    if 'donations' in modified_data:
        modified_data['donations'] = reduce_value(modified_data['donations'])

    if 'donationsReceived' in modified_data:
        modified_data['donationsReceived'] = reduce_value(modified_data['donationsReceived'])

    # Modificar tropas, héroes, equipamiento y hechizos
    if 'troops' in modified_data and isinstance(modified_data['troops'], list):
        modify_troops(modified_data['troops'])

    if 'heroes' in modified_data and isinstance(modified_data['heroes'], list):
        modify_heroes(modified_data['heroes'])

    if 'heroEquipment' in modified_data and isinstance(modified_data['heroEquipment'], list):
        modify_hero_equipment(modified_data['heroEquipment'])

    if 'spells' in modified_data and isinstance(modified_data['spells'], list):
        modify_spells(modified_data['spells'])

    return modified_data

def sanitize_tag(tag):
    """Limpia el tag del jugador para usarlo como nombre de archivo"""
    return tag.replace('#', '').replace('/', '_').replace('\\', '_')

def save_player_json(player_data, output_dir, filename):
    """Guarda un JSON de jugador en el directorio especificado (opcional)"""
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(player_data, f, indent=2, ensure_ascii=False)

# =====================================================================
# FUNCIÓN PRINCIPAL DE INGESTA CON HISTORIAL
# =====================================================================
def ingest_players_with_history():
    """
    Obtiene los jugadores del clan, genera su historial y los inserta en Snowflake.

    - Timestamp original: 2025-11-22 14:47:50.286
    - Timestamps antiguos: reducción aleatoria de 4-8 días por iteración
    - Los timestamps son globales por iteración (no varían entre jugadores)
    """
    print(f"\n{'='*70}")
    print(f"INICIANDO INGESTA CON HISTORIAL")
    print(f"{'='*70}")
    print(f"Timestamp original: {ORIGINAL_TIMESTAMP}")
    print(f"Iteraciones: {NUM_ITERATIONS}")
    print(f"{'='*70}\n")

    # Obtener miembros del clan
    members = get_clan_members(CLAN_TAG)
    print(f"Total de miembros encontrados: {len(members)}\n")

    # Conectar a Snowflake
    conn = get_sf_connection()
    cs = conn.cursor()

    # Generar timestamps globales para cada iteración (4-8 días hacia atrás)
    iteration_timestamps = []
    current_timestamp = ORIGINAL_TIMESTAMP

    for i in range(1, NUM_ITERATIONS + 1):
        # Reducir de 4 a 8 días aleatoriamente
        days_back = random.randint(4, 8)
        current_timestamp = current_timestamp - timedelta(days=days_back)
        iteration_timestamps.append(current_timestamp)

    # Invertir para que old1 sea el más reciente y oldN el más antiguo
    iteration_timestamps.reverse()

    print("Timestamps globales generados:")
    print(f"  Original: {ORIGINAL_TIMESTAMP}")
    for i, ts in enumerate(iteration_timestamps, 1):
        print(f"  old{i}: {ts}")
    print()

    players_for_attacks = []
    total_inserts = 0

    try:
        for idx, m in enumerate(members, 1):
            tag = m["tag"]
            clean_tag = sanitize_tag(tag)
            print(f"[{idx}/{len(members)}] Procesando: {m.get('name', 'Unknown')} ({tag})")

            # Obtener datos completos del jugador (datos actuales)
            player = get_player(tag)

            # 1. INSERTAR DATOS ORIGINALES con timestamp fijo
            print(f"  → Insertando datos originales ({ORIGINAL_TIMESTAMP})...")
            cs.execute("""
                INSERT INTO player_raw (player_tag, raw, ingest_ts)
                SELECT %s, PARSE_JSON(%s), %s
            """, (tag, json.dumps(player), ORIGINAL_TIMESTAMP))
            total_inserts += 1

            # Guardar JSON original (opcional)
            if OUTPUT_DIR:
                save_player_json(player, OUTPUT_DIR, f"{clean_tag}_original.json")

            # 2. GENERAR E INSERTAR ITERACIONES HISTÓRICAS
            current_data = player
            for i in range(1, NUM_ITERATIONS + 1):
                # Modificar el perfil basándose en la iteración anterior
                current_data = modify_player_profile(current_data)

                # Usar el timestamp global de esta iteración
                iteration_ts = iteration_timestamps[i - 1]

                print(f"  → Insertando old{i} ({iteration_ts})...")
                cs.execute("""
                    INSERT INTO player_raw (player_tag, raw, ingest_ts)
                    SELECT %s, PARSE_JSON(%s), %s
                """, (tag, json.dumps(current_data), iteration_ts))
                total_inserts += 1

                # Guardar JSON iteración (opcional)
                if OUTPUT_DIR:
                    save_player_json(current_data, OUTPUT_DIR, f"{clean_tag}_old{i}.json")

            # Agregar a lista de jugadores para ataques
            players_for_attacks.append({
                "player_tag": tag,
                "name": player["name"]
            })

            print(f"  ✓ Completado ({NUM_ITERATIONS + 1} registros insertados)\n")

        # Commit de todas las inserciones
        conn.commit()
        print(f"\n{'='*70}")
        print(f"COMMIT EXITOSO")
        print(f"{'='*70}")

    except Exception as e:
        print(f"\n{'='*70}")
        print(f"ERROR DURANTE LA INGESTA")
        print(f"{'='*70}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        print("ROLLBACK ejecutado")

    finally:
        cs.close()
        conn.close()

    # Resumen final
    print(f"\n{'='*70}")
    print(f"RESUMEN FINAL")
    print(f"{'='*70}")
    print(f"Total de jugadores procesados: {len(members)}")
    print(f"Registros por jugador: {NUM_ITERATIONS + 1} (1 original + {NUM_ITERATIONS} iteraciones)")
    print(f"Total de registros insertados: {total_inserts}")
    print(f"Jugadores en lista de ataques: {len(players_for_attacks)}")
    if OUTPUT_DIR:
        print(f"Archivos JSON guardados en: {OUTPUT_DIR}/")
    print(f"{'='*70}\n")

    return players_for_attacks

# ----------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------
if __name__ == "__main__":
    try:
        players = ingest_players_with_history()
        print("✓ Proceso completado exitosamente")
    except Exception as e:
        print(f"✗ Error durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
