import csv
import json
from pathlib import Path

# 📁 Archivos de entrada
USERS_TSV = "../01_staff_list/output/usuarios.tsv"
SERVICE_POINTS_JSON = "../02_service_points_list/output/service_points.json"
SERVICE_POINT_USERS_JSON = "../03_service_points_staff_list/output/service_point_users.json"
OUTPUT_TSV = "usuarios_con_service_points.tsv"

# 1. Cargar usuarios TSV
def load_users(path):
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        return list(reader)

# 2. Cargar service-points-users JSON
def load_service_point_users(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
        return data  # lista de objetos con userId y servicePointsIds

# 3. Cargar service points JSON
def load_service_points(path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
        return data  # lista de service points

# 4. Crear mapas
def build_user_to_sp_ids_map(sp_users):
    user_to_sp = {}
    for entry in sp_users:
        user_id = entry.get("userId")
        sp_ids = entry.get("servicePointsIds", [])
        if user_id:
            user_to_sp[user_id] = sp_ids
    return user_to_sp

def build_sp_id_to_name_map(service_points):
    sp_id_to_name = {}
    for sp in service_points:
        sp_id_to_name[sp["id"]] = sp.get("name", "")
    return sp_id_to_name

# 5. Agregar nombres de service points a cada usuario
def add_service_points_column(users, user_to_sp_ids, sp_id_to_name):
    for user in users:
        user_id = user["id"]
        sp_ids = user_to_sp_ids.get(user_id, [])
        sp_names = [sp_id_to_name.get(sp_id, "") for sp_id in sp_ids]
        user["servicePoints"] = ", ".join(sp_names)
    return users

# 6. Guardar nuevo TSV
def save_users_with_service_points(users, path):
    fieldnames = list(users[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(users)

# 🧠 Ejecutar todo
def main():
    print("Cargando datos...")
    users = load_users(USERS_TSV)
    sp_users = load_service_point_users(SERVICE_POINT_USERS_JSON)
    service_points = load_service_points(SERVICE_POINTS_JSON)

    print("Procesando relaciones...")
    user_to_sp_ids = build_user_to_sp_ids_map(sp_users)
    sp_id_to_name = build_sp_id_to_name_map(service_points)

    print("Enlazando usuarios con service points...")
    updated_users = add_service_points_column(users, user_to_sp_ids, sp_id_to_name)

    print("Guardando archivo final...")
    save_users_with_service_points(updated_users, OUTPUT_TSV)

    print(f"✅ Archivo guardado: {OUTPUT_TSV}")

if __name__ == "__main__":
    main()
