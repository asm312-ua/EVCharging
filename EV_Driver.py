from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import json
from time import sleep
import sys
import os
import base64
from threading import Thread
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# ============================================================
# Configuración topics
# ============================================================
TOPIC_SOLICITUDES_DRIVER = "solicitudes_driver"  # Driver produce
TOPIC_RESPUESTAS_CENTRAL = "respuestas_central"  # Driver escucha
barrier = True

# ============================================================
# Callback de envío
# ============================================================
def confirmacion_envio(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"[Callback] Mensaje enviado a '{msg.topic()}'")


# ============================================================
# Encriptación / Desencriptación AES-GCM
# ============================================================

AES_KEY_HEX = '4afb208ed9eb14c124c61f4c69ae67293126dd26e7c0d6ea45ca052ceec6557d'
AES_KEY = bytes.fromhex(AES_KEY_HEX)
aesgcm = AESGCM(AES_KEY)

def encriptar_mensaje(diccionario):
    """Convierte dict -> JSON bytes -> AES Encrypt -> Base64 string"""
    try:
        data_bytes = json.dumps(diccionario).encode('utf-8')
        nonce = os.urandom(12)  # El nonce debe ser único por mensaje
        ciphertext = aesgcm.encrypt(nonce, data_bytes, None)
        # Concatenamos nonce + ciphertext y lo pasamos a base64 para enviarlo como texto
        return base64.b64encode(nonce + ciphertext).decode('utf-8')
    except Exception as e:
        print(f"Error encriptando: {e}")
        return None

def desencriptar_mensaje(b64_str):
    """
    Intenta desencriptar el mensaje. 
    Es híbrido: si detecta JSON plano (empieza por {), lo devuelve directo.
    """
    try:
        if not b64_str: 
            return None
        
        b64_str = b64_str.strip()
        
        # 1. Intento de lectura directa (si la Central envió texto plano)
        if b64_str.startswith('{'):
            return json.loads(b64_str)

        # 2. Limpieza de comillas si vienen extra (a veces pasa con JSON strings)
        if b64_str.startswith('"') and b64_str.endswith('"'):
            b64_str = b64_str[1:-1]
        
        # 3. Corrección de padding Base64
        missing_padding = len(b64_str) % 4
        if missing_padding:
            b64_str += '=' * (4 - missing_padding)

        # 4. Desencriptación
        data = base64.b64decode(b64_str)
        nonce = data[:12]
        ciphertext = data[12:]
        original_bytes = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(original_bytes.decode('utf-8'))
        
    except Exception as e:
        # Si falla (por ejemplo, clave incorrecta o basura), retornamos None
        # print(f"Error desencriptando: {e}") 
        return None

# ============================================================
# Hilo que escucha respuestas de la central
# ============================================================
def escuchar_respuestas(broker, driver_id):
    consumer_config = {
        'bootstrap.servers': broker,
        'group.id': f'driver-{driver_id}',
        'auto.offset.reset': 'latest' # Usamos latest para no leer mensajes viejos basura
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_RESPUESTAS_CENTRAL])
    global barrier
    print(f"[{driver_id}] Escuchando respuestas  en '{TOPIC_RESPUESTAS_CENTRAL}'...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
                continue

            # Obtenemos el mensaje crudo (string)
            msg_texto = msg.value().decode('utf-8')
            
            # INTENTAMOS DESENCRIPTAR
            data = desencriptar_mensaje(msg_texto)

            # Si devuelve None es que no era para nosotros o estaba corrupto
            if data is None:
                continue

            # Filtrar solo las respuestas para este driver
            if data.get('driver_id', '').lower() != driver_id.lower():
                continue

            estado = data.get('estado', '').lower()
            cp_id = data.get('cp_id', 'unknown')

            # 🔸 Caso especial: ticket final recibido
            if estado == 'ticket_final':
                kwh = float(data.get('kwh', 0))
                coste = float(data.get('cost', 0))
                precio_kwh = data.get('precio_kwh') # Puede ser None si viene del engine directo

                print("\n============= TICKET FINAL DE CARGA =============")
                print(f"Punto de carga: {cp_id}")
                print(f"Energía suministrada: {kwh:.2f} kWh")
                if precio_kwh:
                    print(f"Precio por kWh: {precio_kwh} €/kWh")
                print(f"IMPORTE TOTAL: {coste:.2f} €")
                print("==================================================\n")
                barrier = False

            # 🔹 Otros mensajes de la central (Autorización, errores, etc.)
            else:
                mensaje_mostrar = data.get('mensaje', estado)
                print(f"[RESPUESTA] CP={cp_id} -> {mensaje_mostrar}")

    except KeyboardInterrupt:
        print(f"\n[DRIVER {driver_id}] Finalizando recepción de mensajes...")
    finally:
        consumer.close()


# ============================================================
# Manejo de solicitudes
# ============================================================
def manejo_solicitudes(producer, driver_id, fichero=None):
    global barrier
    # HAY FICHERO
    if fichero:
        if not os.path.exists(fichero):
            print(f"Error: el fichero '{fichero}' no existe.")
            sys.exit(1)

        with open(fichero, 'r') as f:
            cp_list = [line.strip() for line in f if line.strip()]

        for cp_id in cp_list:
            enviar_solicitud(producer, driver_id, cp_id)
            while barrier:
                sleep(1)
            barrier = True
        print(f"[{driver_id}] Se han enviado correctamente todas las solicitudes.")
    
    # NO HAY FICHERO
    else:
        while True:
            try:
                cp_id = input(f"[{driver_id}] Introduce el ID del punto de recarga (o 'exit'): ")
                if cp_id.lower() == 'exit': break
                enviar_solicitud(producer, driver_id, cp_id)
                # Esperamos a que termine esa carga antes de pedir otra (opcional)
                while barrier:
                    sleep(0.5)
                barrier = True
            except KeyboardInterrupt:
                break

    producer.flush()


# ============================================================
# Envío de una solicitud
# ============================================================
def enviar_solicitud(producer, driver_id, cp_id):
    mensaje = {'driver_id': driver_id, 'cp_id': cp_id}
    
    # AHORA ENCRIPTAMOS EL MENSAJE
    mensaje_cifrado = encriptar_mensaje(mensaje)
    
    if mensaje_cifrado:
        producer.produce(TOPIC_SOLICITUDES_DRIVER, value=mensaje_cifrado, callback=confirmacion_envio)
        producer.poll(0)
        print(f"[{driver_id}] Solicitud enviada a CP: {cp_id}")
    else:
        print(f"[{driver_id}] Error al cifrar solicitud.")


# ============================================================
# Inicialización del driver
# ============================================================
def iniciar_driver(broker, driver_id, fichero=None):
    print(f"[{driver_id}] EV_Driver iniciado (Modo Seguro AES-GCM).")
    print(f"[{driver_id}] Conectado al broker Kafka en {broker}")

    producer = Producer({'bootstrap.servers': broker})

    # Lanzar hilo para escuchar respuestas
    hilo_respuestas = Thread(target=escuchar_respuestas, args=(broker, driver_id))
    hilo_respuestas.daemon = True
    hilo_respuestas.start()

    # Enviar solicitudes
    manejo_solicitudes(producer, driver_id, fichero)
    
    # Si es modo interactivo, el join no es necesario porque el while True lo mantiene vivo,
    # pero si es fichero, esperamos al hilo.
    if fichero:
        hilo_respuestas.join()


# ============================================================
# Ejecución
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Uso: python EV_Driver.py <broker> <driver_id> [fichero]")
        sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]
    fichero = None
    
    if len(sys.argv) == 4:
        fichero = sys.argv[3]

    iniciar_driver(broker, driver_id, fichero)