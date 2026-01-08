from flask import Flask, request, jsonify
import secrets
import json
import os
from datetime import datetime
from threading import Thread, Lock
import time

app = Flask(__name__)

REGISTRY_PORT = 8080
DB_FILE = 'basedatos.json'

db_lock = Lock()
TIMEOUT_CP = 20  # segundos


def cargar_db():
    if not os.path.exists(DB_FILE):
        return {"cps": {}}
    with open(DB_FILE, 'r') as f:
        return json.load(f)


def guardar_db(data):
    with open(DB_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def gestionar_token_db(cp_id, ip_actual):
    with db_lock:
        data = cargar_db()

        if cp_id not in data['cps']:
            print(f"[Registry] Registrando NUEVO CP: {cp_id}")
            data['cps'][cp_id] = {
                "ubicacion": "Desconocida",
                "estado": "ACTIVO",
                "healthy": True,
                "fecha_registro": datetime.now().isoformat()
            }

        cp_data = data['cps'][cp_id]

        if 'token' not in cp_data:
            cp_data['token'] = secrets.token_hex(8)
            print(f"[Registry] Token generado para {cp_id}")

        cp_data['ip'] = ip_actual
        cp_data['ultimo_inicio'] = datetime.now().isoformat()

        guardar_db(data)

        return cp_data['token']


@app.route('/register', methods=['POST'])
def registrar_cp():
    datos = request.get_json()
    cp_id = datos.get('cp_id')

    if not cp_id:
        return jsonify({'error': 'Falta cp_id'}), 400

    ip_origen = request.remote_addr
    token = gestionar_token_db(cp_id, ip_origen)

    return jsonify({
        'status': 'ok',
        'cp_id': cp_id,
        'token': token
    })


def limpiador_cps():
    while True:
        time.sleep(30)  # revisa cada 30 segundos

        with db_lock:
            data = cargar_db()
            ahora = datetime.now()

            cps_a_eliminar = []

            for cp_id, cp_data in data['cps'].items():
                ultimo = cp_data.get('ultimo_inicio')
                if not ultimo:
                    continue

                tiempo_cp = datetime.fromisoformat(ultimo)
                diferencia = (ahora - tiempo_cp).total_seconds()

                if diferencia >= TIMEOUT_CP:
                    cps_a_eliminar.append(cp_id)

            for cp_id in cps_a_eliminar:
                print(f"[LIMPIADOR] Eliminando CP inactivo: {cp_id}")
                del data['cps'][cp_id]

            if cps_a_eliminar:
                guardar_db(data)


if __name__ == '__main__':
    # Hilo limpiador
    Thread(target=limpiador_cps, daemon=True).start()

    try:
        app.run(
            host='0.0.0.0',
            port=REGISTRY_PORT,
            threaded=True,
            ssl_context=('server.crt', 'server.key')
        )
    except FileNotFoundError:
        print("AVISO: Sin SSL, usando HTTP")
        app.run(
            host='0.0.0.0',
            port=REGISTRY_PORT,
            threaded=True
        )
