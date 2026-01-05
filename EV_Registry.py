from flask import Flask, request, jsonify
import secrets
import json
import os
from datetime import datetime  # <--- 1. NUEVO IMPORT

app = Flask(__name__)
REGISTRY_PORT = 8080
DB_FILE = 'basedatos.json'

def gestionar_token_db(cp_id, ip_actual):
    # Cargar BBDD
    if not os.path.exists(DB_FILE):
        data = {"cps": {}} # Inicializa estructura básica si no existe
    else:
        with open(DB_FILE, 'r') as f:
            data = json.load(f)

    # Obtenemos la fecha y hora actual

    # Buscar o Crear CP
    if cp_id not in data['cps']:
        print(f"[Registry] Registrando NUEVO punto de carga: {cp_id}")
        data['cps'][cp_id] = {
            "ubicacion": "Desconocida",
            "estado": "ACTIVO",
            "healthy": True,
            "fecha_registro": datetime.now().isoformat()
        }
    
    # Referencia al CP
    cp_data = data['cps'][cp_id]

    # Asignar Token si no tiene
    if 'token' not in cp_data:
        token_nuevo = secrets.token_hex(8) 
        cp_data['token'] = token_nuevo
        print(f"[Registry] Token generado para {cp_id}: {token_nuevo}")

    # Actualizar datos cambiantes (IP y Último Inicio)
    cp_data['ip'] = ip_actual
    cp_data['ultimo_inicio'] = datetime.now().isoformat()

    # Guardar cambios
    with open(DB_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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
    }), 200

if __name__ == '__main__':
    # Contexto SSL
    try:
        app.run(host='0.0.0.0', port=REGISTRY_PORT, ssl_context=('server.crt', 'server.key'))
    except FileNotFoundError:
        print("AVISO: Sin certificados SSL. Usando HTTP inseguro.")
        app.run(host='0.0.0.0', port=REGISTRY_PORT)