import socket
import json
import sys
import time
import threading
import base64
import os
import requests
import urllib3
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# ============================================================
# Validación de argumentos
# ============================================================
if len(sys.argv) < 6:
    print("Uso: python EV_CP_M.py <cp_id> <engine_host> <engine_port> <central_host> <cmd_port> <IP_registry>")
    print("Ej: python EV_CP_M.py CP01 127.0.0.1 5000 127.0.0.1 6002 127.0.0.1")
    sys.exit(1)

CP_ID = sys.argv[1]
ENGINE_HOST = sys.argv[2]
ENGINE_PORT = int(sys.argv[3])
CENTRAL_HOST = sys.argv[4]
CENTRAL_PORT_ESTADOS = 6000
MONITOR_CMD_PORT = int(sys.argv[5])
REGISTRY_HOST = sys.argv[6] if len(sys.argv) > 6 else "127.0.0.1"
REGISTRY_URL = f"https://{REGISTRY_HOST}:8080"

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
# Constantes y estado global
# ============================================================
SOCKET_TIMEOUT = 2
SOCKET_BUFFER = 8192

# La Central puede establecer una orden persistente (override)
# Posibles valores: None | 'activate' | 'sleep'
central_override = None


# ============================================================
# Utilidades generales
# ============================================================
def obtener_ip_local() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

# ============================================================
# Registro (Obtener el Token)
# ============================================================
def obtener_credenciales():
    global SESSION_TOKEN
    print(f"[Monitor] Conectando a Registry ({REGISTRY_URL})...")
    try:
        resp = requests.post(f"{REGISTRY_URL}/register", json={"cp_id": CP_ID}, verify=False, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            SESSION_TOKEN = data.get('token') # Guardamos la contraseña
            print(f"[Monitor] Login correcto. Token recibido: {SESSION_TOKEN}")
            return True
        else:
            print(f"[Monitor] Error registro: {resp.text}")
            return False
    except Exception as e:
        print(f"[Monitor] Fallo conexión Registry: {e}")
        return False

# ============================================================
# Comunicación con el Engine
# ============================================================
def obtener_estado_engine() -> dict:
    global central_override
    try:
        with socket.create_connection((ENGINE_HOST, ENGINE_PORT), timeout=SOCKET_TIMEOUT) as s:
            s.settimeout(5.0)
            data = s.recv(SOCKET_BUFFER)
            if not data:
                raise RuntimeError("sin datos del Engine")

            estado = json.loads(data.decode('utf-8'))
            engine_healthy = bool(estado.get('healthy'))
            action_to_send = None
            deferred = False

            # Determinar la acción según override o estado
            if central_override == 'sleep':
                action_to_send = 'sleep'
            elif central_override == 'activate':
                if engine_healthy:
                    action_to_send = 'activate'
                else:
                    # Si el Engine no está saludable, aplazar activación
                    action_to_send = 'sleep'
                    deferred = True
            else:
                # Sin override: comportamiento automático
                action_to_send = 'activate' if engine_healthy else 'sleep'

            # Enviar la acción al Engine
            try:
                s.sendall(json.dumps({'action': action_to_send}).encode('utf-8'))
                s.settimeout(2.0)
                ack = s.recv(SOCKET_BUFFER)
                if ack:
                    print(f"[Monitor {CP_ID}] ACK Engine: {ack.decode(errors='ignore')}")
            except Exception:
                pass

            # Completar estado con metadatos para enviar a la Central
            estado.update({
                'ip': obtener_ip_local(),
                'cmd_port': MONITOR_CMD_PORT,
                'cp_id': CP_ID,
                'action_sent': action_to_send,
                'central_override': central_override,
                'override_deferred': deferred,
                'token': SESSION_TOKEN,
            })
            return estado

    except Exception as e:
        # Si el Engine no responde, informar estado de fallo
        print(f"[Monitor {CP_ID}] Error leyendo Engine: {e}")
        return {
            'cp_id': CP_ID,
            'healthy': False,
            'in_use': False,
            'ip': obtener_ip_local(),
            'cmd_port': MONITOR_CMD_PORT,
            'active': central_override != 'sleep',
            'action_sent': 'none',
            'central_override': central_override,
            'override_deferred': False,
            'token': SESSION_TOKEN,
        }


# ============================================================
# Comunicación con la Central
# ============================================================
def enviar_a_central(estado: dict):
    try:
        with socket.create_connection((CENTRAL_HOST, CENTRAL_PORT_ESTADOS), timeout=SOCKET_TIMEOUT) as s:
            # --- CAMBIO AQUÍ ---
            msg_encrypted = encriptar_mensaje(estado)
            msg_final = msg_encrypted + '\n'
            print(f"[Monitor {CP_ID}] Enviando estado encriptado a Central")
            s.sendall(msg_final.encode('utf-8'))
            # -------------------
    except Exception as e:
        print(f"[Monitor {CP_ID}] Error al enviar a CENTRAL: {e}")

# ============================================================
# Recepción de comandos de la Central
# ============================================================
def manejar_comando_central(conn: socket.socket, addr):
    global central_override
    
    # Buffer para acumular datos si llegan fragmentados
    buffer = ''
    
    with conn:
        while True:
            try:
                data = conn.recv(SOCKET_BUFFER)
                if not data:
                    break
                buffer += data.decode('utf-8')
                
                # Procesamos MIENTRAS haya saltos de línea en el buffer
                while '\n' in buffer:
                    mensaje_b64, buffer = buffer.split('\n', 1)
                    mensaje_b64 = mensaje_b64.strip()
                    
                    if not mensaje_b64: continue

                    # 1. Desencriptar
                    msg = desencriptar_mensaje(mensaje_b64)
                    
                    if msg is None:
                        print(f"[Monitor {CP_ID}] Error: Recibido mensaje indescifrable.")
                        continue # Saltamos al siguiente mensaje

                    # 2. Procesar Orden
                    action = (msg.get('action', '') or '').lower()
                    cp = msg.get('cp_id') or CP_ID
                    print(f"[Monitor {CP_ID}] Orden recibida: '{action}'")

                    if action == 'activate':
                        central_override = 'activate'
                    elif action == 'errorlog':
                        print(f"[Monitor {CP_ID}] Error Auth. Durmiendo...")
                        central_override = 'sleep'
                    elif action in ('sleep', 'off'):
                        central_override = 'sleep'
                    elif action in ('clear', 'none', ''):
                        central_override = None

                    # 3. Enviar Respuesta (ACK)
                    # ¡IMPORTANTE! Añadimos '\n' al final para que Central sepa dónde acaba
                    respuesta = {
                        'status': 'ok',
                        'central_override': central_override
                    }
                    ack_encriptado = encriptar_mensaje(respuesta)
                    conn.sendall((ack_encriptado + '\n').encode('utf-8'))
                    
                    return # Salimos tras procesar la orden y responder

            except Exception as e:
                print(f"[Monitor {CP_ID}] Error en socket comandos: {e}")
                break


def servidor_comandos():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', MONITOR_CMD_PORT))
        s.listen(4)
        print(f"[Monitor {CP_ID}] Escuchando comandos de Central en puerto {MONITOR_CMD_PORT}")

        while True:
            conn, addr = s.accept()
            threading.Thread(
                target=manejar_comando_central,
                args=(conn, addr),
                daemon=True
            ).start()


# ============================================================
# Bucle principal
# ============================================================
def main():
    if not obtener_credenciales():
        print("No se puede iniciar sin token del Registry.")
        sys.exit(1)


    print(f"[Monitor {CP_ID}] Iniciado con central_override={central_override}")
    threading.Thread(target=servidor_comandos, daemon=True).start()

    try:
        while True:
            estado = obtener_estado_engine()
            enviar_a_central(estado)
            time.sleep(1)  # frecuencia de actualización
    except KeyboardInterrupt:
        print("\n[Monitor] Apagando...")


# ============================================================
# Ejecución
# ============================================================
if __name__ == '__main__':
    main()