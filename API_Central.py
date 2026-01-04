from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import os
import threading
import time
from datetime import datetime

# ============================================================
# Configuración
# ============================================================
app = Flask(__name__)
CORS(app)

# ÚNICA BASE DE DATOS - Fuente de verdad del sistema
FICHERO_BASE_DATOS = "basedatos.json"

# Estado en memoria para acceso rápido
estado_sistema = {
    'cps': {},
    'ultima_actualizacion': None
}

lock_estado = threading.Lock()


# ============================================================
# Funciones de persistencia
# ============================================================
def cargar_datos():
    """Carga TODA la información desde basedatos.json"""
    global estado_sistema
    
    if not os.path.exists(FICHERO_BASE_DATOS):
        print("[API_Central] No se encontró basedatos.json. Creando estructura vacía...")
        estado_sistema = {
            'cps': {},
            'drivers': {},
            'transacciones': [],
            'alertas_climaticas': {},
            'ultima_actualizacion': datetime.now().isoformat()
        }
        guardar_datos()
        return
    
    try:
        with open(FICHERO_BASE_DATOS, 'r') as f:
            data = json.load(f)
        
        # Asegurar que tiene todas las secciones necesarias
        estado_sistema['cps'] = data.get('cps', {})
        estado_sistema['drivers'] = data.get('drivers', {})
        estado_sistema['transacciones'] = data.get('transacciones', [])
        estado_sistema['alertas_climaticas'] = data.get('alertas_climaticas', {})
        estado_sistema['ultima_actualizacion'] = datetime.now().isoformat()
        
        print(f"[API_Central] Base de datos cargada: {len(estado_sistema['cps'])} CPs")
    except Exception as e:
        print(f"[API_Central] Error al cargar datos: {e}")


def guardar_datos():
    """Guarda todo en basedatos.json"""
    try:
        with open(FICHERO_BASE_DATOS, 'w') as f:
            # Solo guardamos cps porque la Central los gestiona
            # drivers, transacciones y alertas son temporales (en RAM)
            data_to_save = {
                'cps': estado_sistema.get('cps', {}),
                'drivers': estado_sistema.get('drivers', {}),
                'transacciones': estado_sistema.get('transacciones', []),
                'alertas_climaticas': estado_sistema.get('alertas_climaticas', {})
            }
            json.dump(data_to_save, f, indent=2)
    except Exception as e:
        print(f"[API_Central] Error al guardar datos: {e}")


def actualizar_datos_periodicamente():
    """Hilo para recargar datos periódicamente"""
    while True:
        time.sleep(3)  # Recargar cada 3 segundos
        with lock_estado:
            cargar_datos()


# ============================================================
# ENDPOINTS - Charging Points (CPs)
# ============================================================
@app.route('/api/cps', methods=['GET'])
def get_all_cps():
    """Obtiene el estado de todos los CPs"""
    with lock_estado:
        cps_con_alertas = {}
        for cp_id, cp_data in estado_sistema['cps'].items():
            cp_info = cp_data.copy()
            ubicacion = cp_data.get('ubicacion', 'Desconocida')
            
            # Agregar info de alerta climática si existe
            if ubicacion in estado_sistema['alertas_climaticas']:
                cp_info['alerta_climatica'] = estado_sistema['alertas_climaticas'][ubicacion]
            else:
                cp_info['alerta_climatica'] = None
            
            cps_con_alertas[cp_id] = cp_info
        
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'total': len(cps_con_alertas),
            'cps': cps_con_alertas
        }), 200


@app.route('/api/cps/<cp_id>', methods=['GET'])
def get_cp(cp_id):
    """Obtiene el estado de un CP específico"""
    with lock_estado:
        if cp_id not in estado_sistema['cps']:
            return jsonify({
                'status': 'error',
                'mensaje': f'CP {cp_id} no encontrado'
            }), 404
        
        cp_info = estado_sistema['cps'][cp_id].copy()
        ubicacion = cp_info.get('ubicacion', 'Desconocida')
        
        if ubicacion in estado_sistema['alertas_climaticas']:
            cp_info['alerta_climatica'] = estado_sistema['alertas_climaticas'][ubicacion]
        else:
            cp_info['alerta_climatica'] = None
        
        return jsonify({
            'status': 'ok',
            'cp_id': cp_id,
            'data': cp_info
        }), 200


@app.route('/api/cps/<cp_id>', methods=['PUT'])
def update_cp(cp_id):
    """Actualiza información de un CP"""
    data = request.get_json()
    
    with lock_estado:
        if cp_id not in estado_sistema['cps']:
            return jsonify({
                'status': 'error',
                'mensaje': f'CP {cp_id} no encontrado'
            }), 404
        
        # Actualizar campos permitidos
        campos_actualizables = ['ubicacion', 'precio_kwh']
        for campo in campos_actualizables:
            if campo in data:
                estado_sistema['cps'][cp_id][campo] = data[campo]
        
        guardar_datos()
        
        return jsonify({
            'status': 'ok',
            'mensaje': f'CP {cp_id} actualizado',
            'data': estado_sistema['cps'][cp_id]
        }), 200


@app.route('/api/cps/<cp_id>', methods=['DELETE'])
def delete_cp(cp_id):
    """Elimina un CP del sistema"""
    with lock_estado:
        if cp_id not in estado_sistema['cps']:
            return jsonify({
                'status': 'error',
                'mensaje': f'CP {cp_id} no encontrado'
            }), 404
        
        del estado_sistema['cps'][cp_id]
        guardar_datos()
        
        return jsonify({
            'status': 'ok',
            'mensaje': f'CP {cp_id} eliminado'
        }), 200


# ============================================================
# ENDPOINTS - Drivers
# ============================================================
@app.route('/api/drivers', methods=['GET'])
def get_all_drivers():
    """Obtiene el estado de todos los drivers"""
    with lock_estado:
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'total': len(estado_sistema['drivers']),
            'drivers': estado_sistema['drivers']
        }), 200


@app.route('/api/drivers/<driver_id>', methods=['GET'])
def get_driver(driver_id):
    """Obtiene el estado de un driver específico"""
    with lock_estado:
        if driver_id not in estado_sistema['drivers']:
            return jsonify({
                'status': 'error',
                'mensaje': f'Driver {driver_id} no encontrado'
            }), 404
        
        return jsonify({
            'status': 'ok',
            'driver_id': driver_id,
            'data': estado_sistema['drivers'][driver_id]
        }), 200


# ============================================================
# ENDPOINTS - Transacciones
# ============================================================
@app.route('/api/transacciones', methods=['GET'])
def get_transacciones():
    """Obtiene todas las transacciones activas"""
    with lock_estado:
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'total': len(estado_sistema['transacciones']),
            'transacciones': estado_sistema['transacciones']
        }), 200


@app.route('/api/transacciones/<cp_id>', methods=['GET'])
def get_transaccion_cp(cp_id):
    """Obtiene la transacción activa de un CP específico"""
    with lock_estado:
        transaccion = next(
            (t for t in estado_sistema['transacciones'] if t.get('cp_id') == cp_id),
            None
        )
        
        if not transaccion:
            return jsonify({
                'status': 'error',
                'mensaje': f'No hay transacción activa en CP {cp_id}'
            }), 404
        
        return jsonify({
            'status': 'ok',
            'transaccion': transaccion
        }), 200


# ============================================================
# ENDPOINTS - Alertas Climáticas (EV_W)
# ============================================================
@app.route('/api/alertas', methods=['GET'])
def get_alertas():
    """Obtiene todas las alertas climáticas activas"""
    with lock_estado:
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'total': len(estado_sistema['alertas_climaticas']),
            'alertas': estado_sistema['alertas_climaticas']
        }), 200


@app.route('/api/alertas/<ubicacion>', methods=['GET'])
def get_alerta_ubicacion(ubicacion):
    """Obtiene la alerta de una ubicación específica"""
    with lock_estado:
        if ubicacion not in estado_sistema['alertas_climaticas']:
            return jsonify({
                'status': 'ok',
                'ubicacion': ubicacion,
                'alerta': None,
                'mensaje': 'No hay alertas activas para esta ubicación'
            }), 200
        
        return jsonify({
            'status': 'ok',
            'ubicacion': ubicacion,
            'alerta': estado_sistema['alertas_climaticas'][ubicacion]
        }), 200


@app.route('/api/alertas', methods=['POST'])
def crear_alerta():
    """Crea o actualiza una alerta climática (usado por EV_W)"""
    data = request.get_json()
    
    if 'ubicacion' not in data:
        return jsonify({
            'status': 'error',
            'mensaje': 'Falta el campo ubicacion'
        }), 400
    
    ubicacion = data['ubicacion']
    
    with lock_estado:
        alerta = {
            'ubicacion': ubicacion,
            'tipo': data.get('tipo', 'warning'),
            'severidad': data.get('severidad', 'media'),
            'temperatura': data.get('temperatura'),
            'condiciones': data.get('condiciones', 'Condiciones adversas'),
            'mensaje': data.get('mensaje', f'Alerta climática en {ubicacion}'),
            'timestamp': datetime.now().isoformat(),
            'activa': True
        }
        
        estado_sistema['alertas_climaticas'][ubicacion] = alerta
        guardar_datos()
        
        # Buscar CPs afectados
        cps_afectados = [
            cp_id for cp_id, cp_data in estado_sistema['cps'].items()
            if cp_data.get('ubicacion') == ubicacion
        ]
        
        print(f"[API_Central] Alerta creada para {ubicacion}. CPs afectados: {cps_afectados}")
        
        return jsonify({
            'status': 'ok',
            'mensaje': f'Alerta creada para {ubicacion}',
            'alerta': alerta,
            'cps_afectados': cps_afectados
        }), 201


@app.route('/api/alertas/<ubicacion>', methods=['DELETE'])
def cancelar_alerta(ubicacion):
    """Cancela una alerta climática"""
    with lock_estado:
        if ubicacion not in estado_sistema['alertas_climaticas']:
            return jsonify({
                'status': 'error',
                'mensaje': f'No existe alerta para {ubicacion}'
            }), 404
        
        del estado_sistema['alertas_climaticas'][ubicacion]
        guardar_datos()
        
        print(f"[API_Central] Alerta cancelada para {ubicacion}")
        
        return jsonify({
            'status': 'ok',
            'mensaje': f'Alerta cancelada para {ubicacion}'
        }), 200


# ============================================================
# ENDPOINTS - Estado General del Sistema
# ============================================================
@app.route('/api/estado', methods=['GET'])
def get_estado_sistema():
    """Obtiene el estado completo del sistema (dashboard)"""
    with lock_estado:
        # Calcular estadísticas
        cps_activos = sum(1 for cp in estado_sistema['cps'].values() if cp.get('healthy', False))
        cps_en_uso = sum(1 for cp in estado_sistema['cps'].values() if cp.get('in_use', False))
        
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'estadisticas': {
                'total_cps': len(estado_sistema['cps']),
                'cps_activos': cps_activos,
                'cps_en_uso': cps_en_uso,
                'cps_disponibles': cps_activos - cps_en_uso,
                'drivers_activos': len(estado_sistema['drivers']),
                'transacciones_activas': len(estado_sistema['transacciones']),
                'alertas_climaticas': len(estado_sistema['alertas_climaticas'])
            },
            'cps': estado_sistema['cps'],
            'drivers': estado_sistema['drivers'],
            'transacciones': estado_sistema['transacciones'],
            'alertas': estado_sistema['alertas_climaticas']
        }), 200


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check del API"""
    return jsonify({
        'status': 'ok',
        'servicio': 'API_Central',
        'timestamp': datetime.now().isoformat()
    }), 200


# ============================================================
# Inicialización y ejecución
# ============================================================
def iniciar_api(puerto=5000, host='0.0.0.0'):
    """Inicia el servidor API"""
    print(f"[API_Central] Cargando datos desde {FICHERO_BASE_DATOS}...")
    cargar_datos()
    
    # Iniciar hilo de actualización periódica
    threading.Thread(target=actualizar_datos_periodicamente, daemon=True).start()
    
    print(f"[API_Central] Servidor iniciado en http://{host}:{puerto}")
    print(f"[API_Central] Endpoints disponibles:")
    print(f"  GET    /api/estado              - Estado completo del sistema")
    print(f"  GET    /api/cps                 - Listar todos los CPs")
    print(f"  GET    /api/cps/<cp_id>         - Obtener CP específico")
    print(f"  PUT    /api/cps/<cp_id>         - Actualizar CP")
    print(f"  DELETE /api/cps/<cp_id>         - Eliminar CP")
    print(f"  GET    /api/drivers             - Listar drivers activos")
    print(f"  GET    /api/transacciones       - Listar transacciones")
    print(f"  GET    /api/alertas             - Listar alertas climáticas")
    print(f"  POST   /api/alertas             - Crear alerta (EV_W)")
    print(f"  DELETE /api/alertas/<ubicacion> - Cancelar alerta")
    
    app.run(host=host, port=puerto, debug=False, threaded=True)


if __name__ == '__main__':
    iniciar_api()