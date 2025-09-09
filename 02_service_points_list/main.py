import asyncio
import httpx
import config
from connection import Connection
import json
import csv
from pathlib import Path

async def get_all_service_points(base_url, tenant, token):
    service_points = []
    limit = 1000
    offset = 0

    headers = {
        "x-okapi-token": token,
        "x-okapi-tenant": tenant
    }

    async with httpx.AsyncClient() as client:
        while True:
            print(f"Descargando service points... offset: {offset}")
            params = {
                "limit": limit,
                "offset": offset
            }

            try:
                response = await client.get(
                    f"{base_url}/service-points",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                batch = data.get("servicepoints", [])
                service_points.extend(batch)

                if len(batch) < limit:
                    break
                offset += limit

            except httpx.HTTPStatusError as exc:
                print(f"Error HTTP {exc.response.status_code}: {exc.response.text}")
                break
            except httpx.RequestError as exc:
                print(f"Error de conexión: {exc}")
                break

    return service_points

def flatten_service_point(sp):
    """Convierte la estructura de un service-point en una plana para TSV"""
    return {
        "id": sp.get("id"),
        "name": sp.get("name"),
        "code": sp.get("code"),
        "discoveryDisplayName": sp.get("discoveryDisplayName"),
        "description": sp.get("description"),
        "shelvingLagTime": sp.get("shelvingLagTime"),
        "pickupLocation": sp.get("pickupLocation"),
        "holdShelfExpiryPeriod": json.dumps(sp.get("holdShelfExpiryPeriod"), ensure_ascii=False),
        "staffSlips": json.dumps(sp.get("staffSlips"), ensure_ascii=False),
        "metadata.createdDate": sp.get("metadata", {}).get("createdDate"),
        "metadata.updatedDate": sp.get("metadata", {}).get("updatedDate"),
        "metadata.createdByUserId": sp.get("metadata", {}).get("createdByUserId"),
        "metadata.updatedByUserId": sp.get("metadata", {}).get("updatedByUserId")
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
        writer.writerow(["id", "discoveryDisplayName"])
        for d in data:
            writer.writerow([d.get("id"), d.get("discoveryDisplayName")])


async def main():
    base_url = config.OKAPI_URL
    tenant = config.OKAPI_TENANT
    conn = Connection()
    token = await conn.get_token()

    print("Iniciando descarga de service points...")
    service_points = await get_all_service_points(base_url, tenant, token)
    print(f"Service points encontrados: {len(service_points)}")

    Path("output").mkdir(exist_ok=True)

    save_json(service_points, "output/service_points.json")
    save_tsv(service_points, "output/service_points.tsv", flatten_service_point)
    save_uuids(service_points, "output/uuids.tsv")

    print("Archivos guardados en la carpeta 'output'.")

if __name__ == "__main__":
    asyncio.run(main())
