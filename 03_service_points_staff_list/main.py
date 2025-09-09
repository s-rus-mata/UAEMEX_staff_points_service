import asyncio
import httpx
import config
from connection import Connection
import json
import csv
from pathlib import Path

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

    print("Iniciando descarga de service point users...")
    service_point_users = await get_all_service_point_users(base_url, tenant, token)
    print(f"Service point users encontrados: {len(service_point_users)}")

    Path("output").mkdir(exist_ok=True)

    save_json(service_point_users, "output/service_point_users.json")
    save_tsv(service_point_users, "output/service_point_users.tsv", flatten_service_point_user)
    save_uuids(service_point_users, "output/service_point_users_uuids.tsv")

    print("Archivos guardados en la carpeta 'output'.")

if __name__ == "__main__":
    asyncio.run(main())
