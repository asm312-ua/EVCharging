import socket
import threading
import json
import time
import os
import pprint
import subprocess
import sys
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM



# ============================================================
# Configuración de Seguridad (AES-GCM)
# ============================================================
# Esta clave debe ser SECRETA y la misma en Central y Monitor.
# Aquí usamos una hardcodeada para el ejemplo (32 bytes en hex).
AES_KEY_HEX = '4afb208ed9eb14c124c61f4c69ae67293126dd26e7c0d6ea45ca052ceec6557d'
AES_KEY = bytes.fromhex(AES_KEY_HEX)
aesgcm = AESGCM(AES_KEY)

def encriptar_mensaje(diccionario):
    """Convierte dict -> JSON bytes -> AES Encrypt -> Base64 string"""
    data_bytes = json.dumps(diccionario).encode('utf-8')
    nonce = os.urandom(12)  # El nonce debe ser único por mensaje
    ciphertext = aesgcm.encrypt(nonce, data_bytes, None)
    # Concatenamos nonce + ciphertext y lo pasamos a base64 para enviarlo como texto
    return base64.b64encode(nonce + ciphertext).decode('utf-8')

def desencriptar_mensaje(b64_str):
    """Base64 string -> AES Decrypt -> JSON bytes -> dict"""
    try:
        data = base64.b64decode(b64_str)
        nonce = data[:12]      # Extraemos los primeros 12 bytes (nonce)
        ciphertext = data[12:] # El resto es el mensaje cifrado
        original_bytes = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(original_bytes.decode('utf-8'))
    except Exception as e:
        print(f"[Crypto] Error desencriptando: {e}")
        return None

# ============================================================
# Configuración global
# ============================================================
DB_FILE = 'basedatos.json'

CENTRAL_HOST = 'localhost'
CENTRAL_PORT_ESTADOS = 6000
CENTRAL_PORT_SOLICITUDES = 6001
SOCKET_BUFFER = 8192

# Kafka topics
KAFKA_BROKER = sys.argv[1] if len(sys.argv) > 1 else 'localhost:9092'
TOPIC_SOLICIT_DRIVER = 'solicitudes_driver'
TOPIC_SOLICIT_CP = 'peticiones_carga'
TOPIC_SOLICIT_ENGINE = 'peticiones_engine'
TOPIC_TICKETS = 'tickets_cp'
TOPIC_RESPUESTAS = 'respuestas_central'

# Estado global compartido
estados_cp = {}
lock_estados = threading.Lock()
pp = pprint.PrettyPrinter(indent=4)

# Proceso del API
api_process = None
sistema_corriendo = True

FICHERO_BASE_DATOS = "basedatos.json"


# ============================================================
# SISTEMA DE AUDITORÍA
# ============================================================
def registrar_auditoria(origen_ip, accion, descripcion, parametros=None):
    """
    Registra un evento en el sistema de auditoría
    
    Args:
        origen_ip: IP de la máquina que genera el evento
        accion: Tipo de acción (ej: "SOLICITUD_CARGA", "CP_REGISTRADO", etc.)
        descripcion: Descripción detallada del evento
        parametros: Datos adicionales del evento (dict)
    """
    try:
        with open(FICHERO_BASE_DATOS, "r") as f:
            data = json.load(f)
        
        if 'auditoria' not in data:
            data['auditoria'] = []
        
        # Crear entrada de auditoría
        entrada = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'fecha': datetime.now().strftime('%Y-%m-%d'),
            'hora': datetime.now().strftime('%H:%M:%S'),
            'origen_ip': origen_ip,
            'accion': accion,
            'descripcion': descripcion,
            'parametros': parametros or {}
        }
        
        # Añadir al inicio (más recientes primero)
        data['auditoria'].insert(0, entrada)
        
        # Limitar a últimas 200 entradas
        if len(data['auditoria']) > 200:
            data['auditoria'] = data['auditoria'][:200]
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data, f, indent=2)
        
        # Mostrar en terminal
        print(f"[AUDITORÍA] [{entrada['timestamp']}] {origen_ip} → {accion}: {descripcion}")
        
    except Exception as e:
        print(f"[Central] Error en auditoría: {e}")


# ============================================================
# BASE DE DATOS UNIFICADA
# ============================================================
def cargar_cps_basedatos():
    if not os.path.exists(FICHERO_BASE_DATOS):
        print("[Central] No se ha encontrado basedatos.json. Creando estructura inicial...")
        data = {
            'cps': {},
            'drivers': {},
            'transacciones': [],
            'alertas_climaticas': {},
            'auditoria': []
        }
        guardar_datos_completos(data)
        registrar_auditoria('localhost', 'SISTEMA_INICIADO', 'Base de datos creada')
        return {}
    
    try:
        with open(FICHERO_BASE_DATOS, "r") as f:
            datos = json.load(f)
        
        cps = datos.get('cps', {})
        print(f"[Central] Base de datos cargada ({len(cps)} CPs).")
        registrar_auditoria('localhost', 'SISTEMA_INICIADO', f'Base de datos cargada con {len(cps)} CPs')
        return cps
    except Exception as e:
        print(f"[Central] Error al cargar base de datos: {e}")
        return {}


def guardar_datos():
    """Guarda haciendo MERGE para no borrar campos de otros procesos"""
    try:
        # 1. Leer lo que hay en disco actualmente
        datos_disco = {}
        if os.path.exists(FICHERO_BASE_DATOS):
            with open(FICHERO_BASE_DATOS, 'r') as f:
                datos_disco = json.load(f)

        # 2. Actualizar con lo que tenemos en memoria (API)
        #    Esto preserva 'auditoria', 'tokens' y campos que el API no toca.
        datos_disco['cps'].update(estado_sistema.get('cps', {}))
        datos_disco['drivers'] = estado_sistema.get('drivers', {})
        datos_disco['transacciones'] = estado_sistema.get('transacciones', [])
        datos_disco['alertas_climaticas'] = estado_sistema.get('alertas_climaticas', {})
        
        # NOTA: No tocamos 'auditoria' aquí, así que se queda como está en el disco.

        # 3. Guardar todo
        with open(FICHERO_BASE_DATOS, 'w') as f:
            json.dump(datos_disco, f, indent=2)
            
    except Exception as e:
        print(f"[API_Central] Error al guardar datos: {e}")


def guardar_cps_basedatos(estados_en_memoria):
    """
    Combina la lógica de guardado:
    1. Lee toda la base de datos (para no perder auditoría ni drivers).
    2. Actualiza los CPs mezclando datos (para no perder tokens).
    """
    # Aseguramos que usamos la variable global del nombre del fichero
    archivo = FICHERO_BASE_DATOS 
    
    try:
        # --- PASO 1: LEER EL ESTADO ACTUAL DEL DISCO ---
        if os.path.exists(archivo):
            with open(archivo, "r") as f:
                # Cargamos todo (cps, drivers, auditoria, etc.)
                data_completa = json.load(f)
        else:
            # Si no existe, creamos la estructura base
            data_completa = {
                'cps': {}, 
                'drivers': {}, 
                'transacciones': [], 
                'alertas_climaticas': {},
                'auditoria': []
            }

        # Referencia directa a la sección de CPs del disco
        db_cps = data_completa.get('cps', {})

        # --- PASO 2: MERGE INTELIGENTE (MEMORIA -> DISCO) ---
        for cp_id, datos_memoria in estados_en_memoria.items():
            
            if cp_id not in db_cps:
                # CASO A: Es un CP nuevo que no estaba en disco -> Lo guardamos todo
                db_cps[cp_id] = datos_memoria
            else:
                # CASO B: El CP ya existe -> Actualizamos SOLO lo volátil
                # Esto protege el 'token' y la 'fecha_registro' que están en disco
                cp_disco = db_cps[cp_id]
                
                cp_disco['estado'] = datos_memoria.get('estado')
                cp_disco['healthy'] = datos_memoria.get('healthy')
                cp_disco['in_use'] = datos_memoria.get('in_use')
                cp_disco['ip'] = datos_memoria.get('ip')
                cp_disco['cmd_port'] = datos_memoria.get('cmd_port')
                cp_disco['ultimo_inicio'] = datetime.now().isoformat()
                
                # Si cambiaste precio o ubicación manualmente desde el menú, también se guardan
                if 'precio_kwh' in datos_memoria:
                    cp_disco['precio_kwh'] = datos_memoria['precio_kwh']
                if 'ubicacion' in datos_memoria:
                    cp_disco['ubicacion'] = datos_memoria['ubicacion']

        # Actualizamos la sección de CPs en la estructura completa
        data_completa['cps'] = db_cps

        # --- PASO 3: GUARDAR EN DISCO ---
        with open(archivo, "w") as f:
            json.dump(data_completa, f, indent=2)
            
    except Exception as e:
        print(f"[Central] Error crítico al guardar base de datos: {e}")


def limpiar_datos_temporales():
    """Limpia drivers, transacciones, alertas y auditoría al cerrar"""
    try:
        # Primero registrar el cierre antes de limpiar
        registrar_auditoria('localhost', 'SISTEMA_DETENIDO', 'Central apagada - Limpiando datos temporales')
        
        with open(FICHERO_BASE_DATOS, "r") as f:
            data = json.load(f)
        
        # Mantener solo CPs (limpiar drivers, transacciones, alertas)
        # La auditoría se mantiene para histórico
        data['drivers'] = {}
        data['transacciones'] = []
        data['alertas_climaticas'] = {}  # ← Limpiar alertas al cerrar
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data, f, indent=2)
        
        print("[Central] ✓ Datos temporales limpiados (drivers, transacciones, alertas)")
    except Exception as e:
        print(f"[Central] Error al limpiar datos temporales: {e}")


def actualizar_drivers(driver_id, estado, origen_ip='unknown'):
    """Actualiza el estado de un driver en la base de datos"""
    try:
        with open(FICHERO_BASE_DATOS, "r") as f:
            data = json.load(f)
        
        if 'drivers' not in data:
            data['drivers'] = {}
        
        data['drivers'][driver_id] = {
            'driver_id': driver_id,
            'estado': estado,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'ip': origen_ip
        }
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data, f, indent=2)
        
        # Auditoría
        registrar_auditoria(
            origen_ip, 
            'CAMBIO_ESTADO_DRIVER', 
            f'Driver {driver_id} cambió a estado: {estado}',
            {'driver_id': driver_id, 'nuevo_estado': estado}
        )
        
    except Exception as e:
        print(f"[Central] Error al actualizar driver: {e}")


def actualizar_transacciones(driver_id, cp_id, accion='inicio', origen_ip='unknown'):
    """Actualiza las transacciones en la base de datos"""
    try:
        with open(FICHERO_BASE_DATOS, "r") as f:
            data = json.load(f)
        
        if 'transacciones' not in data:
            data['transacciones'] = []
        
        if accion == 'inicio':
            data['transacciones'].append({
                'driver_id': driver_id,
                'cp_id': cp_id,
                'inicio': time.strftime('%Y-%m-%d %H:%M:%S'),
                'estado': 'en_curso'
            })
            
            # Auditoría
            registrar_auditoria(
                origen_ip,
                'TRANSACCION_INICIADA',
                f'Carga iniciada: {driver_id} en {cp_id}',
                {'driver_id': driver_id, 'cp_id': cp_id}
            )
            
        elif accion == 'fin':
            data['transacciones'] = [
                t for t in data['transacciones']
                if not (t.get('driver_id') == driver_id and t.get('cp_id') == cp_id)
            ]
            
            # Auditoría
            registrar_auditoria(
                origen_ip,
                'TRANSACCION_FINALIZADA',
                f'Carga finalizada: {driver_id} en {cp_id}',
                {'driver_id': driver_id, 'cp_id': cp_id}
            )
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[Central] Error al actualizar transacciones: {e}")

# ============================================================
# Funcion auxiliar: validar credenciales de un CP
# ============================================================
def validar_credenciales(cp_id, token_recibido):
    """
    Comprueba en basedatos.json si el CP existe y si el token coincide.
    Retorna True si es válido, False si es un impostor.
    """
    if not os.path.exists(DB_FILE):
        return False # Si no hay base de datos, nadie es válido
        
    try:
        with open(DB_FILE, 'r') as f:
            db = json.load(f)
            
        # 1. ¿Existe el CP en la base de datos?
        if cp_id not in db.get('cps', {}):
            return False
            
        # 2. ¿El token almacenado coincide con el recibido?
        token_real = db['cps'][cp_id].get('token')
        
        # Comparamos (usando strings para evitar errores de None)
        if str(token_real) == str(token_recibido):
            return True
        else:
            return False
            
    except Exception as e:
        print(f"[Central] Error leyendo BBDD para validar: {e}")
        return False
# ============================================================
# INICIAR API_CENTRAL AUTOMÁTICAMENTE
# ============================================================
def iniciar_api_central():
    """Inicia el API_Central en un proceso separado"""
    global api_process
    try:
        print("[Central] Iniciando API_Central...")
        api_process = subprocess.Popen(
            [sys.executable, 'API_Central.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        print(f"[Central] API_Central iniciado (PID: {api_process.pid})")
        print("[Central] API disponible en http://localhost:5000")
        registrar_auditoria('localhost', 'API_INICIADO', f'API_Central iniciado (PID: {api_process.pid})')
        return api_process
    except Exception as e:
        print(f"[Central] Error al iniciar API_Central: {e}")
        registrar_auditoria('localhost', 'ERROR_API', f'Error al iniciar API: {e}')
        return None


def detener_api_central():
    """Detiene el API_Central al cerrar la Central"""
    global api_process
    if api_process:
        print("[Central] Deteniendo API_Central...")
        try:
            api_process.terminate()
            api_process.wait(timeout=5)
            print("[Central] API_Central detenido.")
            registrar_auditoria('localhost', 'API_DETENIDO', 'API_Central detenido correctamente')
        except Exception as e:
            print(f"[Central] Error al detener API: {e}")
            api_process.kill()


# ============================================================
# KAFKA: inicialización
# ============================================================
def inicializar_kafka():
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'central-unificada-group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([TOPIC_SOLICIT_CP, TOPIC_TICKETS, TOPIC_SOLICIT_DRIVER, TOPIC_SOLICIT_ENGINE])
        print(f"[Central] Kafka inicializado (broker: {KAFKA_BROKER})")
        registrar_auditoria('localhost', 'KAFKA_INICIADO', f'Kafka inicializado en {KAFKA_BROKER}')
        return producer, consumer
    except Exception as e:
        print(f"[Central] Aviso: Kafka no disponible: {e}")
        registrar_auditoria('localhost', 'ERROR_KAFKA', f'Kafka no disponible: {e}')
        return None, None


# ============================================================
# Gestor de estados de CP (desde monitor por sockets)
# ============================================================
def manejar_estado_cp(conn, addr):
    buffer = ''
    origen_ip = addr[0]
    
    with conn:
        while True:
            data = conn.recv(SOCKET_BUFFER)
            if not data:
                break
            buffer += data.decode()
            while '\n' in buffer:
                mensaje_b64, buffer = buffer.split('\n', 1) # Recibimos B64
                if not mensaje_b64.strip(): continue
                
                # --- CAMBIO AQUÍ: Desencriptar ---
                state = desencriptar_mensaje(mensaje_b64)
                if state is None:
                    continue # Si falla la desencriptación, ignoramos
                cp_id = state.get('cp_id')
                if not validar_credenciales(cp_id,state.get('token')):
                    print(f"[Central] Intento de conexión no autorizado desde {addr} \n Credenciales recibidas: \ncp_id={cp_id} \ntoken={state.get('token')}")
                    conn.close()
                    enviar_orden(state.get('cp_id'), 'ErrorLog')  # Ordenar al CP que se desconecte
                    return
                try:

                    if not cp_id:
                        continue
                    
                    with lock_estados:
                        if cp_id not in estados_cp:
                            print(f"\n[Central] ✓ Registrado nuevo CP: {cp_id} desde {origen_ip}")
                            estados_cp[cp_id] = {
                                "ubicacion": "Desconocida",
                                "precio_kwh": 0.3,
                                "estado": "DESCONECTADO",
                                "healthy": False,
                                "in_use": False,
                            }
                            
                            # Auditoría de registro
                            registrar_auditoria(
                                origen_ip,
                                'CP_REGISTRADO',
                                f'Nuevo CP registrado: {cp_id}',
                                {'cp_id': cp_id, 'healthy': state.get('healthy', False)}
                            )

                        # Actualizar estado
                        estado_anterior = estados_cp[cp_id].get('estado')
                        healthy_anterior = estados_cp[cp_id].get('healthy')
                        
                        estados_cp[cp_id]['estado'] = "ACTIVO" if state.get('healthy', False) else "DESCONECTADO"
                        estados_cp[cp_id]['healthy'] = state.get('healthy', False)
                        estados_cp[cp_id]['in_use'] = state.get('in_use', False)
                        estados_cp[cp_id]['ip'] = state.get('ip', origen_ip)
                        estados_cp[cp_id]['cmd_port'] = state.get('cmd_port')
                        estados_cp[cp_id]['token'] = state.get('token')
                        
                        # Auditoría de cambio de estado significativo
                        if estado_anterior != estados_cp[cp_id]['estado']:
                            registrar_auditoria(
                                origen_ip,
                                'CAMBIO_ESTADO_CP',
                                f'CP {cp_id}: {estado_anterior} → {estados_cp[cp_id]["estado"]}',
                                {'cp_id': cp_id, 'estado_anterior': estado_anterior, 'estado_nuevo': estados_cp[cp_id]['estado']}
                            )
                        
                        if healthy_anterior != estados_cp[cp_id]['healthy']:
                            accion = 'CP_SALUDABLE' if estados_cp[cp_id]['healthy'] else 'CP_NO_SALUDABLE'
                            registrar_auditoria(
                                origen_ip,
                                accion,
                                f'CP {cp_id} cambió healthy: {healthy_anterior} → {estados_cp[cp_id]["healthy"]}',
                                {'cp_id': cp_id, 'healthy': estados_cp[cp_id]['healthy']}
                            )
                        
                        guardar_cps_basedatos(estados_cp)
                        
                except Exception as e:
                    print(f"[Central] Error procesando estado: {e}")
                    registrar_auditoria(origen_ip, 'ERROR_PROCESO_ESTADO', f'Error procesando estado de CP: {e}')


def servidor_estados_cp():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((CENTRAL_HOST, CENTRAL_PORT_ESTADOS))
        s.listen()
        print(f"[Central] Escuchando estados en puerto {CENTRAL_PORT_ESTADOS}")
        while sistema_corriendo:
            try:
                s.settimeout(1.0)
                conn, addr = s.accept()
                threading.Thread(target=manejar_estado_cp, args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue


# ============================================================
# Enviar órdenes a un CP (por socket)
# ============================================================
def enviar_orden(cp_id, action):
    # Obtener IP/Puerto de memoria
    with lock_estados:
        info = estados_cp.get(cp_id)
    
    if not info:
        print(f"[Central] No se conoce el CP {cp_id}")
        return

    try:
        # Conectar con timeout de 3 segundos
        with socket.create_connection((info['ip'], info['cmd_port']), timeout=3) as s:
            
            # 1. Enviar Orden Encriptada + \n
            payload = {'cp_id': cp_id, 'action': action}
            msg_encriptado = encriptar_mensaje(payload)
            s.sendall((msg_encriptado + '\n').encode('utf-8'))
            
            print(f"[Central] → Enviado '{action}' a {cp_id}")

            # 2. Recibir Confirmación (ACK) hasta encontrar \n
            buffer_ack = ''
            while True:
                chunk = s.recv(1024)
                if not chunk: break
                buffer_ack += chunk.decode('utf-8')
                
                if '\n' in buffer_ack:
                    ack_b64, _ = buffer_ack.split('\n', 1)
                    ack_dict = desencriptar_mensaje(ack_b64)
                    
                    if ack_dict:
                        print(f"[Central] ← ACK de {cp_id}: {ack_dict.get('status')} (Override: {ack_dict.get('central_override')})")
                    else:
                        print(f"[Central] ACK corrupto de {cp_id}")
                    break # Ya tenemos respuesta, salimos

    except socket.timeout:
        print(f"[Central] Timeout esperando respuesta de {cp_id}")
    except ConnectionRefusedError:
        print(f"[Central] {cp_id} rechazó la conexión (¿Monitor apagado?)")
    except Exception as e:
        print(f"[Central] Error enviando orden a {cp_id}: {e}")
        registrar_auditoria(info.get('ip', 'unknown'), 'ERROR_ENVIO_ORDEN', f'Fallo al enviar {action}: {e}')


# ============================================================
# VERIFICADOR DE ALERTAS CLIMÁTICAS
# ============================================================
def verificar_alertas_climaticas():
    """Verifica alertas y gestiona los CPs de forma eficiente (sin bloquear)"""
    try:
        # 1. Leemos alertas del disco (Lectura es rápida)
        if not os.path.exists(FICHERO_BASE_DATOS): return
        with open(FICHERO_BASE_DATOS, "r") as f:
            data = json.load(f)
        
        alertas = data.get('alertas_climaticas', {})
        
        # Listas para acumular tareas de red (para hacerlas FUERA del lock)
        ordenes_sleep = []
        ordenes_activate = []
        auditorias_pendientes = []
        cambios_en_memoria = False

        # 2. BLOQUE CRÍTICO: Solo operaciones de memoria (Rapidísimo)
        with lock_estados:
            
            # A) Verificar ACTIVACIÓN de alertas
            for ubicacion, alerta in alertas.items():
                if not alerta.get('activa', False):
                    continue
                
                for cp_id, cp_info in estados_cp.items():
                    if cp_info.get('ubicacion') == ubicacion:
                        # Si no estaba ya en alerta
                        if not cp_info.get('alerta_activa', False):
                            print(f"[Central] ⚠️ Alerta detectada para {cp_id} en {ubicacion}")
                            
                            # Actualizamos memoria
                            estados_cp[cp_id]['estado'] = 'FUERA_DE_SERVICIO'
                            estados_cp[cp_id]['alerta_activa'] = True
                            
                            # Encolamos tareas
                            ordenes_sleep.append(cp_id)
                            auditorias_pendientes.append((
                                cp_info.get('ip', 'unknown'),
                                'CP_FUERA_SERVICIO_ALERTA',
                                f'CP {cp_id} desactivado por alerta en {ubicacion}',
                                {'cp_id': cp_id, 'motivo': alerta.get('mensaje')}
                            ))
                            cambios_en_memoria = True

            # B) Verificar DESACTIVACIÓN de alertas (Restauración)
            for cp_id, cp_info in estados_cp.items():
                if cp_info.get('alerta_activa', False):
                    ubicacion_cp = cp_info.get('ubicacion')
                    # Comprobar si la alerta ya no existe o no está activa
                    if ubicacion_cp not in alertas or not alertas.get(ubicacion_cp, {}).get('activa', False):
                        print(f"[Central] ✓ Alerta finalizada para {cp_id}")
                        
                        # Restauramos estado
                        es_healthy = cp_info.get('healthy', False)
                        estados_cp[cp_id]['estado'] = 'ACTIVO' if es_healthy else 'DESCONECTADO'
                        estados_cp[cp_id]['alerta_activa'] = False
                        
                        # Encolamos tareas
                        if es_healthy:
                            ordenes_activate.append(cp_id)
                        
                        auditorias_pendientes.append((
                            cp_info.get('ip', 'unknown'),
                            'CP_RESTAURADO_ALERTA',
                            f'CP {cp_id} restaurado (fin de alerta)',
                            {'cp_id': cp_id}
                        ))
                        cambios_en_memoria = True
            
            # 3. Si hubo cambios, GUARDAMOS EN DISCO (Una sola vez)
            if cambios_en_memoria:
                guardar_cps_basedatos(estados_cp)

        # ---------------------------------------------------------
        # 4. ZONA LIBRE: Operaciones lentas (Red y Auditoría)
        #    Ya hemos soltado el 'lock', así que el servidor sigue respondiendo a otros.
        # ---------------------------------------------------------
        
        # Enviar órdenes de apagado
        for cp_id in ordenes_sleep:
            enviar_orden(cp_id, 'sleep')
            
        # Enviar órdenes de encendido
        for cp_id in ordenes_activate:
            enviar_orden(cp_id, 'activate')
            
        # Registrar auditorías
        for ip, accion, desc, params in auditorias_pendientes:
            registrar_auditoria(ip, accion, desc, params)

    except Exception as e:
        print(f"[Central] Error verificando alertas: {e}")


def hilo_verificador_alertas():
    """Hilo que verifica alertas cada 5 segundos"""
    while sistema_corriendo:
        verificar_alertas_climaticas()
        time.sleep(5)


# ============================================================
# Menú
# ============================================================
def menu_central():
    while True:
        time.sleep(1)
        print("\n======================== MENU CENTRAL ===========================")
        print("1. Ver estado de CPs")
        print("2. Activar CP")
        print("3. Apagar CP")
        print("4. Registrar/Manejar datos de CPs")
        print("5. Salir")
        print("=================================================================")
        op = input("> ").strip()
        if op == '1':
            with lock_estados:
                print("\nID\tUbicación\t\tPrecio\t\tEstado\t\tHealthy\tIn_Use")
                print("----------------------------------------------------------------------------------")
                for cp, info in estados_cp.items():
                    print(f"{cp}\t{info['ubicacion'][:18]:<18}\t{info['precio_kwh']} €/kWh\t{info['estado']}\t{info['healthy']}\t{info['in_use']}")
        elif op == '2':
            cp_id = input("CP ID: ").strip()
            enviar_orden(cp_id, 'activate')
        elif op == '3':
            cp_id = input("CP ID: ").strip()
            enviar_orden(cp_id, 'sleep')
        elif op == '4':
            cp_id = input("CP ID: ").strip()
            info = estados_cp.get(cp_id)
            if not info:
                print(f"[Central] CP {cp_id} no encontrado.")
                continue
            print(f"{cp_id} → Ubicación actual: {info['ubicacion']} | Precio actual: {info['precio_kwh']}")
            opcion = input("¿Desea cambiar los datos? (S/N): ")
            if opcion == 'S':
                nueva_ubicacion = input(f"Introduce la ubicación de {cp_id}: ").strip()
                nuevo_precio = input(f"Introduce el precio/kWh de {cp_id}: ").strip()
                
                cambios = {}
                if nueva_ubicacion:
                    estados_cp[cp_id]['ubicacion'] = nueva_ubicacion
                    cambios['ubicacion'] = nueva_ubicacion
                if nuevo_precio:
                    try:
                        estados_cp[cp_id]['precio_kwh'] = float(nuevo_precio)
                        cambios['precio_kwh'] = float(nuevo_precio)
                    except ValueError:
                        print("Precio inválido. No se actualizó.")
                
                guardar_cps_basedatos(estados_cp)
                print(f"[Central] CP {cp_id} actualizado.")
                
                # Auditoría
                registrar_auditoria(
                    'localhost',
                    'CP_MODIFICADO_MANUAL',
                    f'CP {cp_id} modificado desde menú central',
                    {'cp_id': cp_id, 'cambios': cambios}
                )
                
        elif op == '5':
            break
        else:
            print("Opción inválida.")


# ============================================================
# KAFKA: funciones auxiliares
# ============================================================
def enviar_respuesta_kafka(producer, driver_id, cp_id, estado, status, precio_kwh=None):
    if producer is None:
        print(f"[KAFKA:{TOPIC_RESPUESTAS}] fallback -> driver={driver_id} cp={cp_id} estado={estado} status={status}")
        return

    payload = {
        'driver_id': driver_id,
        'cp_id': cp_id,
        'estado': estado,
        'mensaje': estado,
        'status': status
    }
    if precio_kwh is not None:
        payload['precio_kwh'] = precio_kwh
    try:
        producer.produce(TOPIC_RESPUESTAS, key=driver_id, value=json.dumps(payload).encode('utf-8'))
        producer.flush(3)
        print(f"[Central] → Respuesta Kafka enviada: driver={driver_id}, status={status}")
    except Exception as e:
        print(f"[Central] Error al producir respuesta Kafka: {e}")


# ============================================================
# KAFKA: procesado de mensajes (CON AUDITORÍA)
# ============================================================
def procesar_mensaje_kafka(producer, topic, data):
    try:
        if topic in (TOPIC_SOLICIT_CP, TOPIC_SOLICIT_DRIVER, TOPIC_SOLICIT_ENGINE):
            driver_id = data.get('driver_id')
            cp_id = data.get('cp_id')
            print(f"[Central][KAFKA] Solicitud recibida: driver={driver_id} cp={cp_id}")

            # Actualizar estado del driver en BD
            actualizar_drivers(driver_id, 'solicitando_carga', 'kafka')

            with lock_estados:
                estado_cp = estados_cp.get(cp_id)
                precio_kwh = estado_cp.get('precio_kwh', 0.30) if estado_cp else 0.30

            if not estado_cp:
                mensaje = f"CP {cp_id} desconocido"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                actualizar_drivers(driver_id, 'error_cp_desconocido', 'kafka')
                
                # Auditoría
                registrar_auditoria(
                    'kafka',
                    'SOLICITUD_DENEGADA_CP_DESCONOCIDO',
                    f'Driver {driver_id} intentó cargar en CP desconocido: {cp_id}',
                    {'driver_id': driver_id, 'cp_id': cp_id}
                )
                return

            if not estado_cp.get('healthy', False):
                mensaje = f"CP {cp_id} no saludable"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                actualizar_drivers(driver_id, 'error_cp_no_disponible', 'kafka')
                
                # Auditoría
                registrar_auditoria(
                    'kafka',
                    'SOLICITUD_DENEGADA_CP_NO_SALUDABLE',
                    f'Carga denegada: CP {cp_id} no saludable',
                    {'driver_id': driver_id, 'cp_id': cp_id}
                )
                return

            if estado_cp.get('in_use', False):
                mensaje = f"CP {cp_id} ocupado"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                actualizar_drivers(driver_id, 'error_cp_ocupado', 'kafka')
                
                # Auditoría
                registrar_auditoria(
                    'kafka',
                    'SOLICITUD_DENEGADA_CP_OCUPADO',
                    f'Carga denegada: CP {cp_id} ya está en uso',
                    {'driver_id': driver_id, 'cp_id': cp_id}
                )
                return

            # Autorizar carga
            mensaje_ok = f"Carga autorizada en {cp_id}"
            enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje_ok, 'ok', precio_kwh)
            
            # Actualizar BD
            actualizar_drivers(driver_id, 'cargando', estado_cp.get('ip', 'unknown'))
            actualizar_transacciones(driver_id, cp_id, 'inicio', estado_cp.get('ip', 'unknown'))
            
            print(f"[Central] Autorizada carga para {driver_id} en {cp_id} con precio: {precio_kwh}")

            # Notificar al Engine
            payload_engine = {
                'driver_id': driver_id,
                'cp_id': cp_id,
                'estado': 'start',
                'mensaje': 'Iniciar carga',
                'status': 'ok',
                'precio_kwh': precio_kwh
            }
            try:
                producer.produce(TOPIC_RESPUESTAS, key=cp_id, value=json.dumps(payload_engine).encode('utf-8'))
                producer.flush(3)
                print(f"[Central] Notificación enviada al Engine {cp_id}")
            except Exception as e:
                print(f"[Central] Error al notificar al Engine: {e}")

            with lock_estados:
                estados_cp[cp_id]['in_use'] = True
                guardar_cps_basedatos(estados_cp)

        elif topic == TOPIC_TICKETS:
            cp_id = data.get('cp_id')
            driver_id = data.get('driver_id', 'unknown')
            kwh = data.get('kwh')
            cost = data.get('cost')

            print(f"[Central] Ticket recibido de {cp_id}: kWh={kwh} cost={cost} driver={driver_id}")

            # Actualizar BD
            with lock_estados:
                cp_ip = estados_cp.get(cp_id, {}).get('ip', 'unknown')
            
            actualizar_drivers(driver_id, 'completado', cp_ip)
            actualizar_transacciones(driver_id, cp_id, 'fin', cp_ip)

            with lock_estados:
                rec = estados_cp.setdefault(cp_id, {})
                rec['last_ticket'] = data
                rec['in_use'] = False
                guardar_cps_basedatos(estados_cp)

            # Reenviar ticket al driver
            payload_ticket = {
                'driver_id': driver_id,
                'cp_id': cp_id,
                'estado': 'ticket_final',
                'mensaje': f'Carga finalizada en {cp_id}',
                'status': 'ok',
                'kwh': kwh,
                'cost': cost
            }

            try:
                producer.produce(TOPIC_RESPUESTAS, key=driver_id, value=json.dumps(payload_ticket).encode('utf-8'))
                producer.flush(3)
                print(f"[Central] Ticket reenviado al driver {driver_id}.")
            except Exception as e:
                print(f"[Central] Error al reenviar ticket: {e}")

    except Exception as e:
        print("[Central] Error procesando mensaje Kafka:", e)


def kafka_worker(producer, consumer):
    if consumer is None:
        return
    print("[Central] kafka_worker activo")
    try:
        while sistema_corriendo:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("[Central] Kafka error:", msg.error())
                continue
            try:
                topic = msg.topic()
                data = json.loads(msg.value().decode('utf-8'))
                procesar_mensaje_kafka(producer, topic, data)
            except Exception as e:
                print("[Central] Error procesando mensaje:", e)
    except Exception as e:
        print("[Central] kafka_worker terminado:", e)


# ============================================================
# Ejecución
# ============================================================
def main():
    global estados_cp, sistema_corriendo
    
    print("[Central] ==============================================")
    print(f"[Central] Iniciando EV_Central (Kafka: {KAFKA_BROKER})")
    print("[Central] ==============================================")
    
    # Cargar base de datos
    estados_cp = cargar_cps_basedatos()
    
    # Iniciar API_Central automáticamente
    iniciar_api_central()
    
    # Inicializar Kafka
    producer, consumer = inicializar_kafka()
    if consumer is not None:
        threading.Thread(target=kafka_worker, args=(producer, consumer), daemon=True).start()
    
    # Servidor de estados
    threading.Thread(target=servidor_estados_cp, daemon=True).start()
    
    # Verificador de alertas
    threading.Thread(target=hilo_verificador_alertas, daemon=True).start()
    
    print("[Central] Servidor iniciado.")
    print("[Central] API REST disponible en http://localhost:5000")
    
    try:
        menu_central()
    except KeyboardInterrupt:
        print("\n[Central] Cerrando sistema...")
    finally:
        sistema_corriendo = False
        limpiar_datos_temporales()
        detener_api_central()
        print("[Central] Sistema apagado.")


if __name__ == '__main__':
    main()