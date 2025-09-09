import asyncio
import httpx
import config
from connection import Connection
import json
import csv
from pathlib import Path

def load_user_ids_from_tsv(path):
    """Carga userIds desde un archivo TSV con o sin encabezado"""
    user_ids = set()
    with open(path, "r", encoding="utf-8") as f:

        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            # Si hay encabezado, lo ignoramos (suponiendo que la columna se llama 'userId')
            if row and row[0].lower() == "userId".lower():
                continue
            if row:
                user_ids.add(row[0].strip())
    return user_ids

async def get_all_service_point_users(base_url, tenant, token):
    service_point_users = []
    limit = 1000
    offset = 0

    headers = {
        "x-okapi-token": token,
        "x-okapi-tenant": tenant
    }

    async with httpx.AsyncClient() as client:
        while True:
            print(f"Descargando service point users... offset: {offset}")
            params = {
                "limit": limit,
                "offset": offset
            }

            try:
                response = await client.get(
                    f"{base_url}/service-points-users",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                batch = data.get("servicePointsUsers", [])
                service_point_users.extend(batch)

                if len(batch) < limit:
                    break
                offset += limit

            except httpx.HTTPStatusError as exc:
                print(f"Error HTTP {exc.response.status_code}: {exc.response.text}")
                break
            except httpx.RequestError as exc:
                print(f"Error de conexión: {exc}")
                break

    return service_point_users

def flatten_service_point_user(spu):
    """Convierte la estructura de un service point user en una plana para TSV"""
    return {
        "id": spu.get("id"),
        "userId": spu.get("userId"),
        "servicePoints": json.dumps(spu.get("servicePoints"), ensure_ascii=False),
        "defaultServicePointId": spu.get("defaultServicePointId"),
    }

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def save_tsv(data, path, flatten_fn):
    flat_data = [flatten_fn(d) for d in data]
    fieldnames = flat_data[0].keys() if flat_data else []

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(flat_data)

def save_uuids(data, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id", "userId"])
        for d in data:
            writer.writerow([d.get("id"), d.get("userId")])

async def main():
    base_url = config.OKAPI_URL
    tenant = config.OKAPI_TENANT
    conn = Connection()
    token = await conn.get_token()

    # Cargar los userId deseados desde usuarios.tsv
    user_ids = load_user_ids_from_tsv("../01_staff_list/output/uuids.tsv")
    print(f"User IDs cargados desde archivo: {len(user_ids)}")

    # Descargar todos los service point users
    print("Iniciando descarga de service point users...")
    all_sp_users = await get_all_service_point_users(base_url, tenant, token)
    print(f"Total service point users encontrados: {len(all_sp_users)}")

    # Filtrar los que están en la lista de userIds
    filtered_sp_users = [spu for spu in all_sp_users if spu.get("userId") in user_ids]
    print(f"Service point users después del filtro: {len(filtered_sp_users)}")

    Path("output").mkdir(exist_ok=True)

    save_json(filtered_sp_users, "output/service_point_users_filtrados.json")
    save_tsv(filtered_sp_users, "output/service_point_users_filtrados.tsv", flatten_service_point_user)
    save_uuids(filtered_sp_users, "output/service_point_users_uuids_filtrados.tsv")

    print("Archivos guardados en la carpeta 'output'.")

if __name__ == "__main__":
    asyncio.run(main())
