import asyncio
import httpx
import config
from connection import Connection
import json
import csv
from pathlib import Path

async def get_users_by_patrongroup(base_url, tenant, token, patron_group_id):
    users = []
    limit = 1000
    offset = 0

    headers = {
        "x-okapi-token": token,
        "x-okapi-tenant": tenant
    }

    async with httpx.AsyncClient() as client:
        while True:
            print(f"Descargando usuarios... offset: {offset}")
            query = f'patronGroup=="{patron_group_id}"'
            params = {
                "query": query,
                "limit": limit,
                "offset": offset
            }

            try:
                response = await client.get(
                    f"{base_url}/users",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                batch = data.get("users", [])
                users.extend(batch)

                if len(batch) < limit:
                    break
                offset += limit

            except httpx.HTTPStatusError as exc:
                print(f"Error HTTP {exc.response.status_code}: {exc.response.text}")
                break
            except httpx.RequestError as exc:
                print(f"Error de conexión: {exc}")
                break

    return users

def flatten_user(user):
    """Convierte la estructura anidada en una plana para TSV"""
    flat = {
        "id": user.get("id"),
        "username": user.get("username"),
        "barcode": user.get("barcode"),
        "active": user.get("active"),
        "patronGroup": user.get("patronGroup"),
        "createdDate": user.get("createdDate"),
        "updatedDate": user.get("updatedDate"),
        "personal.lastName": user.get("personal", {}).get("lastName"),
        "personal.firstName": user.get("personal", {}).get("firstName"),
        "personal.middleName": user.get("personal", {}).get("middleName"),
        "personal.email": user.get("personal", {}).get("email"),
        "personal.mobilePhone": user.get("personal", {}).get("mobilePhone"),
        "personal.preferredContactTypeId": user.get("personal", {}).get("preferredContactTypeId"),
        "metadata.createdDate": user.get("metadata", {}).get("createdDate"),
        "metadata.updatedDate": user.get("metadata", {}).get("updatedDate"),
        "metadata.createdByUserId": user.get("metadata", {}).get("createdByUserId"),
        "metadata.updatedByUserId": user.get("metadata", {}).get("updatedByUserId"),
    }
    return flat

def save_json(users, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)

def save_tsv(users, path):
    flattened_users = [flatten_user(u) for u in users]
    fieldnames = flattened_users[0].keys() if flattened_users else []

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(flattened_users)

def save_uuids(users, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id"])
        for u in users:
            writer.writerow([u.get("id")])

async def main():
    base_url = config.OKAPI_URL
    tenant = config.OKAPI_TENANT
    conn = Connection()
    token = await conn.get_token()
    patron_group_id = "34688b28-fec2-4a8d-b108-e35532f54601"

    print("Iniciando descarga de usuarios...")
    users = await get_users_by_patrongroup(base_url, tenant, token, patron_group_id)
    print(f"Usuarios encontrados con patronGroup {patron_group_id}: {len(users)}")

    # Crear carpeta de salida si no existe
    Path("output").mkdir(exist_ok=True)

    # Guardar los archivos
    save_json(users, "output/usuarios.json")
    save_tsv(users, "output/usuarios.tsv")
    save_uuids(users, "output/uuids.tsv")

    print("Archivos guardados en la carpeta 'output'.")

if __name__ == "__main__":
    asyncio.run(main())
