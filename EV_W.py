import sys
import requests
import json
import time
import threading
from datetime import datetime

# ============================================================
# Configuración
# ============================================================
API_KEY = "2de671acf516add68c98a4a5b5c039d9"  # Tu API key de OpenWeather
URL_API_CENTRAL = "http://localhost:5000"
INTERVALO_CONSULTA = 4

# Umbrales de temperatura
TEMP_MIN = 0   # Por debajo: alerta de frío

# Estado global
temperaturas_actuales = {}  # {ciudad: {"temp": X, "condiciones": "...", "timestamp": ...}}
lock_temperaturas = threading.Lock()
thread_running = True
api_conectado = False


# ============================================================
# Verificar conexión con API_Central
# ============================================================
def verificar_conexion_api():
    global api_conectado
    
    max_intentos = 5
    for intento in range(1, max_intentos + 1):
        try:
            respuesta = requests.get(f"{URL_API_CENTRAL}/api/health", timeout=3)
            if respuesta.status_code == 200:
                print("[EV_W] Conexión con API_Central verificada")
                api_conectado = True
                return True
        except:
            if intento < max_intentos:
                print(f"[EV_W] Reintentando conexión... ({intento}/{max_intentos})")
                time.sleep(2)
            continue
    
    print(f"[EV_W] No se pudo conectar con API_Central en {URL_API_CENTRAL}")
    return False


# ============================================================
# Obtener ubicaciones de CPs desde API_Central
# ============================================================
def obtener_ubicaciones_cps():
    try:
        respuesta = requests.get(f"{URL_API_CENTRAL}/api/cps", timeout=3)
        
        if respuesta.status_code == 200:
            data = respuesta.json()
            cps = data.get('cps', {})
            
            # Extraer ubicaciones únicas
            ubicaciones = set()
            for cp_id, cp_data in cps.items():
                ubicacion = cp_data.get('ubicacion', 'Desconocida')
                if ubicacion != 'Desconocida':
                    ubicaciones.add(ubicacion)
            
            return list(ubicaciones)
        else:
            print(f"[EV_W] Error al obtener CPs: HTTP {respuesta.status_code}")
            return []
            
    except Exception as e:
        print(f"[EV_W] Error al obtener ubicaciones: {e}")
        return []


# ============================================================
# Obtener clima desde OpenWeather
# ============================================================
def obtener_clima(ciudad):
    # Consulta OpenWeatherMap para obtener temperatura y condiciones
    # Returns: (temperatura_celsius, descripcion) o (None, None) si falla

    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
        respuesta = requests.get(url, timeout=5)
        
        if respuesta.status_code == 200:
            datos = respuesta.json()
            temperatura = datos['main']['temp']
            return temperatura
        elif respuesta.status_code == 404:
            print(f"[EV_W] Error: La ciudad '{ciudad}' no existe en OpenWeather.")
            return None, None
        else:
            print(f"[EV_W] Error HTTP {respuesta.status_code} al consultar OpenWeather para {ciudad}")
            return None, None
            
    except requests.exceptions.Timeout:
        print(f"[EV_W] Timeout al consultar OpenWeather para {ciudad}")
        return None, None
    except Exception as e:
        print(f"[EV_W] Error al consultar OpenWeather para {ciudad}: {e}")
        return None, None


# ============================================================
# Gestión de alertas en API_Central
# ============================================================
def crear_alerta_central(ciudad, temperatura):
    try:
        if temperatura < TEMP_MIN:
            tipo = "danger"
            severidad = "alta"
            mensaje = f"Alerta de frío extremo en {ciudad}. CPs fuera de servicio."
        else:
            # No debería llegar aquí, pero por si acaso
            return True
        
        payload = {
            "ubicacion": ciudad,
            "tipo": tipo,
            "severidad": severidad,
            "temperatura": temperatura,
            "mensaje": mensaje
        }
        
        respuesta = requests.post(f"{URL_API_CENTRAL}/api/alertas", json=payload, timeout=3)
        
        if respuesta.status_code in (200, 201):
            data = respuesta.json()
            cps_afectados = data.get('cps_afectados', [])
            print(f"\n[EV_W] ALERTA enviada para {ciudad} ({temperatura}°C)")
            if cps_afectados:
                print(f"[EV_W]    CPs afectados: {', '.join(cps_afectados)}")
            return True
        else:
            print(f"[EV_W] Error al enviar alerta: HTTP {respuesta.status_code}")
            return False
            
    except Exception as e:
        print(f"[EV_W] Error al enviar alerta: {e}")
        return False


def cancelar_alerta_central(ciudad):
    try:
        url = f"{URL_API_CENTRAL}/api/alertas/{ciudad}"
        respuesta = requests.delete(url, timeout=3)
        
        if respuesta.status_code in (200, 404):
            # 404 significa que no había alerta, también es válido
            if respuesta.status_code == 200:
                print(f"[EV_W] Alerta cancelada para {ciudad}")
            return True
        else:
            print(f"[EV_W] Error al cancelar alerta: HTTP {respuesta.status_code}")
            return False
            
    except Exception as e:
        print(f"[EV_W] Error al cancelar alerta: {e}")
        return False


# ============================================================
# Hilo de monitorización automática
# ============================================================
def hilo_monitorizacion():
    # Hilo que consulta automáticamente cada 4 segundos las temperaturas de las ubicaciones de los CPs y gestiona alertas

    global thread_running, temperaturas_actuales, api_conectado
    
    print("[EV_W] Hilo de monitorización iniciado")
    alertas_activas = set()  # Ciudades con alertas activas
    
    while thread_running:
        if not api_conectado:
            time.sleep(INTERVALO_CONSULTA)
            continue
        
        # Obtener ubicaciones actuales de CPs
        ubicaciones = obtener_ubicaciones_cps()
        
        if not ubicaciones:
            time.sleep(INTERVALO_CONSULTA)
            continue
        
        # Consultar temperatura de cada ubicación
        for ciudad in ubicaciones:
            temperatura = obtener_clima(ciudad)
            
            if temperatura is not None:
                # Actualizar temperaturas globales
                with lock_temperaturas:
                    temperaturas_actuales[ciudad] = {
                        "temp": temperatura,
                        "timestamp": datetime.now().strftime('%H:%M:%S')
                    }
                
                # Evaluar si hay alerta
                hay_alerta = (temperatura < TEMP_MIN)
                
                if hay_alerta:
                    if ciudad not in alertas_activas:
                        # Nueva alerta
                        crear_alerta_central(ciudad, temperatura)
                        alertas_activas.add(ciudad)
                    # Si ya existe, no hacer nada (evitar spam)
                else:
                    if ciudad in alertas_activas:
                        # Cancelar alerta existente
                        cancelar_alerta_central(ciudad)
                        alertas_activas.remove(ciudad)
        
        # Esperar 4 segundos antes de la próxima consulta
        time.sleep(INTERVALO_CONSULTA)
    
    print("[EV_W] Hilo de monitorización detenido")


# ============================================================
# Funciones del menú
# ============================================================
def mostrar_temperaturas_cps():
    print("\n=============================================")
    print("     TEMPERATURAS DE UBICACIONES DE CPs")
    print("=============================================")
    
    with lock_temperaturas:
        if not temperaturas_actuales:
            print("No hay datos disponibles aún.")
            return
        
        for ciudad, datos in sorted(temperaturas_actuales.items()):
            temp = datos['temp']
            timestamp = datos['timestamp']
            
            # Determinar emoji según temperatura
            if temp < TEMP_MIN:
                estado = "ALERTA FRÍO"
            else:
                estado = "OK"
            
            print(f"\n{ciudad}")
            print(f"   Temperatura: {temp}°C")
            print(f"   Estado: {estado}")
            print(f"   Última actualización: {timestamp}")


def consultar_temperatura_ciudad():
    ciudad = input("\nIntroduce el nombre de la ciudad: ").strip()
    
    if not ciudad:
        print("Nombre de ciudad vacío.")
        return
    
    print(f"\nConsultando temperatura de {ciudad}...")
    
    temperatura = obtener_clima(ciudad)
    
    if temperatura is not None:
        print(f"\nDatos obtenidos:")
        print(f"   Ciudad: {ciudad}")
        print(f"   Temperatura: {temperatura}°C")
        
        # Mostrar si estaría en alerta
        if temperatura < TEMP_MIN:
            print(f"   Esta temperatura activaría una ALERTA DE FRÍO")
        else:
            print(f"   Temperatura operativa")
    else:
        print(f"No se pudo obtener la temperatura de {ciudad}")


# ============================================================
# Menú principal
# ============================================================
def mostrar_menu():
    print("\n\n=================== Weather Control Office ===================")
    print("1. Mostrar temperaturas de los CPs")
    print("2. Mostrar temperatura de ciudad")
    print("3. Salir")
    print("==============================================================")


def menu_principal():
    global thread_running
    
    while True:
        mostrar_menu()
        opcion = input("Selecciona una opción: ").strip()
        
        if opcion == '1':
            mostrar_temperaturas_cps()
        
        elif opcion == '2':
            consultar_temperatura_ciudad()
        
        elif opcion == '3':
            print("\n[EV_W] Cerrando Weather Control Office...")
            thread_running = False
            
            # Cancelar todas las alertas activas
            print("[EV_W] Cancelando alertas activas...")
            with lock_temperaturas:
                for ciudad in temperaturas_actuales.keys():
                    cancelar_alerta_central(ciudad)
            
            print("[EV_W] Finalizado.\n")
            break
        
        else:
            print("Opción inválida. Elige 1, 2 o 3.")


# ============================================================
# Main
# ============================================================
def main():
    global api_conectado
    
    print("Iniciando [EV_W]")
    print("Configuración actual: ")
    print(f"  API Central: {URL_API_CENTRAL}")
    print(f"  Intervalo de consulta: {INTERVALO_CONSULTA} segundos")
    print(f"  Umbral de alerta: < {TEMP_MIN}°C\n")
    
    # 1. Verificar conexión con API_Central
    print("[EV_W] Paso 1: Verificando conexión con API_Central...")
    if not verificar_conexion_api():
        print("\n[EV_W] ERROR: No se puede conectar con API_Central")
        print("[EV_W] El sistema continuará pero no podrá enviar alertas")
        respuesta = input("[EV_W] ¿Continuar de todas formas? (S/N): ")
        if respuesta != 'S':
            print("[EV_W] Saliendo...")
            sys.exit(1)
        api_conectado = False
    
    # 2. Obtener ubicaciones iniciales
    if api_conectado:
        print("\n[EV_W] Paso 2: Obteniendo ubicaciones de CPs...")
        ubicaciones = obtener_ubicaciones_cps()
        if ubicaciones:
            print(f"[EV_W] Encontradas {len(ubicaciones)} ubicaciones:")
            for ub in ubicaciones:
                print(f"   - {ub}")
        else:
            print("[EV_W] No se encontraron CPs con ubicaciones válidas")
    
    # 3. Iniciar hilo de monitorización automática
    print("\n[EV_W] Paso 3: Iniciando monitorización automática...")
    hilo = threading.Thread(target=hilo_monitorizacion, daemon=True)
    hilo.start()
    print("[EV_W] Hilo de monitorización activado")
    
    # Esperar un momento para que el hilo obtenga datos iniciales
    print("\n[EV_W] Obteniendo temperaturas iniciales...")
    time.sleep(2)
    
    # 4. Mostrar menú interactivo
    print("\n[EV_W] Sistema listo")    
    try:
        menu_principal()
    except KeyboardInterrupt:
        print("\n\n[EV_W] Interrupción detectada. Cerrando...")
        thread_running = False
        
        # Cancelar alertas activas
        with lock_temperaturas:
            for ciudad in temperaturas_actuales.keys():
                cancelar_alerta_central(ciudad)
        
        print("[EV_W] Finalizado.")
        sys.exit(0)


if __name__ == "__main__":
    main()