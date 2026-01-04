import socket
import threading
import json
import time
import os
import pprint
import subprocess
import sys
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
CENTRAL_HOST = 'localhost'
CENTRAL_PORT_ESTADOS = 6000
CENTRAL_PORT_SOLICITUDES = 6001
SOCKET_BUFFER = 8192

# Kafka topics
KAFKA_BROKER = 'localhost:9092'
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


# ============================================================
# BASE DE DATOS UNIFICADA
# ============================================================
FICHERO_BASE_DATOS = "basedatos.json"

def cargar_cps_basedatos():
    """Carga la base de datos unificada"""
    if not os.path.exists(FICHERO_BASE_DATOS):
        print("[Central] No se ha encontrado basedatos.json. Creando estructura inicial...")
        data = {
            'cps': {},
            'drivers': {},
            'transacciones': [],
            'alertas_climaticas': {}
        }
        guardar_cps_basedatos(data)
        return {}
    
    try:
        with open(FICHERO_BASE_DATOS, "r") as f:
            datos = json.load(f)
        
        # Retornar solo la sección de CPs
        cps = datos.get('cps', {})
        print(f"[Central] Base de datos cargada ({len(cps)} CPs).")
        return cps
    except Exception as e:
        print(f"[Central] Error al cargar base de datos: {e}")
        return {}


def guardar_cps_basedatos(data_cps):
    """Guarda los CPs manteniendo el resto de información intacta"""
    try:
        # Leer toda la base de datos
        if os.path.exists(FICHERO_BASE_DATOS):
            with open(FICHERO_BASE_DATOS, "r") as f:
                data_completa = json.load(f)
        else:
            data_completa = {
                'cps': {},
                'drivers': {},
                'transacciones': [],
                'alertas_climaticas': {}
            }
        
        # Si data_cps es solo el dict de CPs, actualizamos solo esa sección
        if isinstance(data_cps, dict) and not any(k in data_cps for k in ['drivers', 'transacciones', 'alertas_climaticas']):
            data_completa['cps'] = data_cps
        else:
            # Si es la estructura completa, guardamos todo
            data_completa = data_cps
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data_completa, f, indent=2)
    except Exception as e:
        print(f"[Central] Error al guardar base de datos: {e}")


def actualizar_drivers(driver_id, estado):
    """Actualiza el estado de un driver en la base de datos"""
    try:
        with open(FICHERO_BASE_DATOS, "r") as f:
            data = json.load(f)
        
        if 'drivers' not in data:
            data['drivers'] = {}
        
        data['drivers'][driver_id] = {
            'driver_id': driver_id,
            'estado': estado,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[Central] Error al actualizar driver: {e}")


def actualizar_transacciones(driver_id, cp_id, accion='inicio'):
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
        elif accion == 'fin':
            data['transacciones'] = [
                t for t in data['transacciones']
                if not (t.get('driver_id') == driver_id and t.get('cp_id') == cp_id)
            ]
        
        with open(FICHERO_BASE_DATOS, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[Central] Error al actualizar transacciones: {e}")


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
        time.sleep(2)  # Dar tiempo a que arranque
        print(f"[Central] API_Central iniciado (PID: {api_process.pid})")
        print("[Central] API disponible en http://localhost:5000")
        return api_process
    except Exception as e:
        print(f"[Central] Error al iniciar API_Central: {e}")
        print("[Central] La Central continuará funcionando sin API.")
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
        print("[Central] Kafka inicializado (producer + consumer).")
        return producer, consumer
    except Exception as e:
        print(f"[Central] Aviso: Kafka no disponible: {e}")
        return None, None


# ============================================================
# Gestor de estados de CP (desde monitor por sockets)
# ============================================================
def manejar_estado_cp(conn, addr):
    buffer = ''
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
                try:
                    cp_id = state.get('cp_id')
                    if not cp_id:
                        continue
                    with lock_estados:
                        if cp_id not in estados_cp:
                            print(f"\n[Central] Registrado un nuevo CP: {cp_id}")
                            estados_cp[cp_id] = {
                                "ubicacion": "Desconocida",
                                "precio_kwh": 0.3,
                                "estado": "DESCONECTADO",
                                "healthy": False,
                                "in_use": False,
                            }

                        estados_cp[cp_id]['estado'] = "ACTIVO" if state.get('healthy', False) else "DESCONECTADO"
                        estados_cp[cp_id]['healthy'] = state.get('healthy', False)
                        estados_cp[cp_id]['in_use'] = state.get('in_use', False)
                        estados_cp[cp_id]['ip'] = state.get('ip', addr[0])
                        estados_cp[cp_id]['cmd_port'] = state.get('cmd_port')
                        guardar_cps_basedatos(estados_cp)
                except Exception as e:
                    print(f"[Central] Error procesando estado: {e}")


def servidor_estados_cp():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((CENTRAL_HOST, CENTRAL_PORT_ESTADOS))
        s.listen()
        print(f"[Central] Escuchando estados en puerto {CENTRAL_PORT_ESTADOS}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=manejar_estado_cp, args=(conn, addr), daemon=True).start()


# ============================================================
# Enviar órdenes a un CP (por socket)
# ============================================================
def enviar_orden(cp_id, action):
    with lock_estados:
        info = estados_cp.get(cp_id)
    if not info:
        print(f"[Central] No se conoce el CP {cp_id}")
        return

    try:
        with socket.create_connection((info['ip'], info['cmd_port']), timeout=3) as s:
            # --- CAMBIO AQUÍ: Encriptar envío ---
            payload = {'cp_id': cp_id, 'action': action}
            msg_encriptado = encriptar_mensaje(payload)
            s.sendall((msg_encriptado + '\n').encode('utf-8')) # Importante añadir \n
            # ------------------------------------

            # Esperar respuesta (ACK) que también vendrá encriptada
            resp_b64 = s.recv(1024)
            resp_dict = desencriptar_mensaje(resp_b64.decode('utf-8'))
            print(f"[Central] Respuesta de {cp_id}: {resp_dict}")
    except Exception as e:
        print(f"[Central] Error al enviar orden a {cp_id}: {e}")


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
                print("\nID\tUbicación\t\tPrecio\t\tEstado\tHealthy\tIn_Use")
                print("----------------------------------------------------------------------------------")
                for cp, info in estados_cp.items():
                    print(f"{cp}\t{info['ubicacion'][:18]:<18}\t{info['precio_kwh']} €/kWh\t{info['estado']}\t{info['healthy']}\t{info['in_use']}")
        elif op == '2':
            enviar_orden(input("CP ID: ").strip(), 'activate')
        elif op == '3':
            enviar_orden(input("CP ID: ").strip(), 'sleep')
        elif op == '4':
            cp_id = input("CP ID: ")
            info = estados_cp.get(cp_id)
            if not info:
                print(f"[Central] CP {cp_id} no encontrado.")
                continue
            print(f"{cp_id} -> Ubicación actual: {info['ubicacion']} | Precio actual: {info['precio_kwh']}")
            opcion = input("¿Desea cambiar los datos? (S/N): ")
            if opcion == 'S':
                nueva_ubicacion = input(f"Introduce la ubicación de {cp_id}: ")
                nuevo_precio = input(f"Introduce el precio/kWh de {cp_id}: ")
                if nueva_ubicacion:
                    estados_cp[cp_id]['ubicacion'] = nueva_ubicacion
                if nuevo_precio:
                    try:
                        estados_cp[cp_id]['precio_kwh'] = float(nuevo_precio)
                    except ValueError:
                        print("Precio inválido. No se actualizó.")
                guardar_cps_basedatos(estados_cp)
                print(f"[Central] CP {cp_id} actualizado.")
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
    except Exception as e:
        print(f"[Central] Error al producir respuesta Kafka: {e}")


# ============================================================
# KAFKA: procesado de mensajes (CON ACTUALIZACIÓN DE BD)
# ============================================================
def procesar_mensaje_kafka(producer, topic, data):
    try:
        if topic in (TOPIC_SOLICIT_CP, TOPIC_SOLICIT_DRIVER, TOPIC_SOLICIT_ENGINE):
            driver_id = data.get('driver_id')
            cp_id = data.get('cp_id')
            print(f"[Central][KAFKA] Solicitud recibida: driver={driver_id} cp={cp_id}")

            # Actualizar estado del driver en BD
            actualizar_drivers(driver_id, 'solicitando_carga')

            with lock_estados:
                estado_cp = estados_cp.get(cp_id)
                precio_kwh = estado_cp.get('precio_kwh', 0.30) if estado_cp else 0.30

            if not estado_cp:
                mensaje = f"CP {cp_id} desconocido"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                actualizar_drivers(driver_id, 'error_cp_desconocido')
                return

            if not estado_cp.get('healthy', False):
                mensaje = f"CP {cp_id} no saludable"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                actualizar_drivers(driver_id, 'error_cp_no_disponible')
                return

            if estado_cp.get('in_use', False):
                mensaje = f"CP {cp_id} ocupado"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                actualizar_drivers(driver_id, 'error_cp_ocupado')
                return

            # Autorizar carga
            mensaje_ok = f"Carga autorizada en {cp_id}"
            enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje_ok, 'ok', precio_kwh)
            
            # Actualizar BD
            actualizar_drivers(driver_id, 'cargando')
            actualizar_transacciones(driver_id, cp_id, 'inicio')
            
            print(f"[Central] Autorizada carga para {driver_id} en {cp_id}")

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

            print(f"[Central] Ticket recibido de {cp_id}: kWh={kwh} cost={cost}")

            # Actualizar BD
            actualizar_drivers(driver_id, 'completado')
            actualizar_transacciones(driver_id, cp_id, 'fin')

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
            except Exception as e:
                print(f"[Central] Error al reenviar ticket: {e}")

    except Exception as e:
        print("[Central] Error procesando mensaje Kafka:", e)


def kafka_worker(producer, consumer):
    if consumer is None:
        return
    print("[Central] kafka_worker activo")
    try:
        while True:
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
    global estados_cp
    
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
    
    print("[Central] Servidor iniciado.")
    print("[Central] API REST disponible en http://localhost:5000")
    
    try:
        menu_central()
    except KeyboardInterrupt:
        print("\n[Central] Cerrando sistema...")
    finally:
        detener_api_central()
        print("[Central] Sistema apagado.")


if __name__ == '__main__':
    main()