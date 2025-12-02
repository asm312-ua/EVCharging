import sys
import requests
import json
import time

# --- CONFIGURACIÓN ---
API_KEY = "PON_AQUI_TU_API_KEY_DE_OPENWEATHER"  # ¡Pega tu API Key aquí!
URL_CENTRAL = "http://localhost:8080/api/weather/alert" # URL de tu Central (invéntatela por ahora si no la tienes)

def obtener_clima(ciudad):
    try:
        # 1. Consultar OpenWeatherMap
        # Usamos units=metric para que nos lo dé directamente en Celsius (más fácil)
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
        res = requests.get(url)
        
        if res.status_code == 200:
            data = res.json()
            temp_celsius = data['main']['temp']
            descripcion = data['weather'][0]['description']
            return temp_celsius, descripcion
        else:
            print(f"Error al obtener clima para {ciudad}: {res.status_code}")
            return None, None
            
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None, None

def notificar_central(ciudad, estado, temperatura):
    # Simulación de envío a la Central
    payload = {
        "source": "EV_W",
        "city": ciudad,
        "status": estado,
        "temperature": temperatura
    }
    
    print(f"--- ENVIANDO A CENTRAL ({ciudad}) ---")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    # Descomenta las siguientes líneas cuando tengas la Central lista
    # try:
    #     requests.post(URL_CENTRAL, json=payload)
    #     print(">> Notificación enviada con éxito.")
    # except:
    #     print(">> Error: No se pudo contactar con la Central.")

def main():
    # 1. Leer el argumento de la consola
    if len(sys.argv) < 2:
        print("Uso: python EV_W.py <NombreCiudad>")
        print("Ejemplo: python EV_W.py Madrid")
        return

    ciudad = sys.argv[1] # El primer argumento (índice 1) es la ciudad

    print(f"Iniciando servicio de clima para: {ciudad}")

    # Bucle infinito (como pide la práctica, cada 4-6 segs)
    while True:
        temp, desc = obtener_clima(ciudad)
        
        if temp is not None:
            print(f"\n[CLIMA ACTUAL] {ciudad}: {temp}ºC ({desc})")
            
            # Lógica de negocio (ejemplo: si hace menos de 0 grados -> Alerta)
            if temp < 0:
                print("¡ALERTA DE FRÍO! Deteniendo cargas...")
                notificar_central(ciudad, "ALERT_COLD", temp)
            elif temp > 45:
                print("¡ALERTA DE CALOR! Peligro de sobrecalentamiento...")
                notificar_central(ciudad, "ALERT_HEAT", temp)
            else:
                print("Temperatura operativa correcta.")
                notificar_central(ciudad, "OK", temp)
        
        # Esperar 5 segundos antes de volver a comprobar
        time.sleep(5)

if __name__ == "__main__":
    main()
