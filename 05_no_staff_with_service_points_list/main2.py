import asyncio
import aiohttp
import json
import csv
from pathlib import Path
from connection import Connection  # Ajusta esto según tu estructura
import config

# Patron group a excluir
EXCLUDED_PATRON_GROUP_ID = "34688b28-fec2-4a8d-b108-e35532f54601"

# Archivo de entrada con relaciones
SERVICE_POINT_USERS_JSON = "../03_service_points_staff_list/output/service_point_users.json"
# Carpeta y archivo de salida
OUTPUT_DIR = Path("output")
OUTPUT_TSV = OUTPUT_DIR / "filtered_users_2.tsv"

async def get_user_info(session, token, user_id):
    url = f"{config.OKAPI_URL}/users/{user_id}"
    headers = {
        "X-Okapi-Tenant": config.OKAPI_TENANT,
        "X-Okapi-Token": token,
        "Content-Type": "application/json"
    }
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            return await resp.json()
        else:
            print(f"⚠️  Error al obtener usuario {user_id}: {resp.status}")
            return None

async def get_service_point_name(session, token, sp_id):
    url = f"{config.OKAPI_URL}/service-points/{sp_id}"
    headers = {
        "X-Okapi-Tenant": config.OKAPI_TENANT,
        "X-Okapi-Token": token,
        "Content-Type": "application/json"
    }
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data.get("name", "")
        else:
            print(f"⚠️  Error al obtener service point {sp_id}: {resp.status}")
            return None


async def get_patron_group_name(session, token, group_id, cache):
  if group_id in cache:
    return cache[group_id]

  url = f"{config.OKAPI_URL}/groups/{group_id}"
  headers = {
    "X-Okapi-Tenant": config.OKAPI_TENANT,
    "X-Okapi-Token": token,
    "Content-Type": "application/json"
  }

  async with session.get(url, headers=headers) as resp:
    if resp.status == 200:
      data = await resp.json()
      group_name = data.get("group", "")
      cache[group_id] = group_name
      return group_name
    else:
      print(f"⚠️  Error al obtener patronGroup {group_id}: {resp.status}")
      return ""


async def process_users(relations, conn):
  async with aiohttp.ClientSession() as session:
    users = []
    service_point_cache = {}
    patron_group_cache = {}

    for rel in relations:
      user_id = rel.get("userId")
      service_point_ids = rel.get("servicePointsIds", [])

      if not user_id or not service_point_ids:
        continue

      user = await get_user_info(session, conn, user_id)
      if not user:
        continue

      patron_group_id = user.get("patronGroup")
      if patron_group_id == EXCLUDED_PATRON_GROUP_ID:
        continue

      # 🔹 Obtener nombre del patronGroup
      patron_group_name = await get_patron_group_name(session, conn, patron_group_id, patron_group_cache)
      user["patronGroupName"] = patron_group_name

      # 🔹 Obtener nombres de service points
      sp_names = []
      for sp_id in service_point_ids:
        if sp_id not in service_point_cache:
          sp_name = await get_service_point_name(session, conn, sp_id)
          service_point_cache[sp_id] = sp_name
        sp_names.append(service_point_cache[sp_id])

      user["servicePointNames"] = "; ".join(sp_names)
      users.append(user)

    return users

def write_users_to_tsv(users, output_path):
    if not users:
        print("⚠️  No hay usuarios para exportar.")
        return

    output_path.parent.mkdir(exist_ok=True)  # Asegurar que la carpeta exista

    fieldnames = list(users[0].keys())

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction='ignore')
        writer.writeheader()
        for user in users:
            writer.writerow(user)

    print(f"✅ TSV exportado: {output_path.resolve()}")

async def main():

    conn = Connection()
    token = await conn.get_token()

    path = Path(SERVICE_POINT_USERS_JSON)
    if not path.exists():
        print(f"❌ Archivo no encontrado: {SERVICE_POINT_USERS_JSON}")
        return

    with open(path, "r", encoding="utf-8") as f:
        relations_data = json.load(f)

    relations = relations_data if isinstance(relations_data, list) else relations_data.get("servicePointUsers", [])

    users = await process_users(relations, token)

    write_users_to_tsv(users, OUTPUT_TSV)

if __name__ == "__main__":
    asyncio.run(main())
