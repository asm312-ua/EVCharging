import sys
import requests
import time

# --- TU CONFIGURACIÓN ---
# Regístrate en openweathermap.org y pega tu API Key aquí abajo entre las comillas:
API_KEY = "2de671acf516add68c98a4a5b5c039d9" 

def obtener_clima(ciudad):
    try:
        # Petición a OpenWeather (units=metric para Celsius)
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
        respuesta = requests.get(url)
        
        if respuesta.status_code == 200:
            datos = respuesta.json()
            temp = datos['main']['temp']
            desc = datos['weather'][0]['description']
            return temp, desc
        elif respuesta.status_code == 404:
            print(f"Error: La ciudad '{ciudad}' no existe.")
            return None, None
        else:
            print(f"Error HTTP: {respuesta.status_code}")
            return None, None
            
    except Exception as e:
        print(f"Error de conexión (¿revisa?): {e}")
        return None, None

def main():
    # 1. Comprobamos si ha pasado un nombre
    if len(sys.argv) < 2:
        print("Introduce la ciudad como argumento")
        print("Uso correcto: python EV_W_Terminal.py Madrid")
        return

    ciudad = sys.argv[1]
    print(f"--- Iniciando Monitor de Clima para: {ciudad.upper()} ---")
    print("Pulsa Ctrl + C para salir.\n")

    # 2. Bucle infinito (cada 5 segundos)
    while True:
        temperatura, descripcion = obtener_clima(ciudad)
        
        if temperatura is not None:
            # Imprimimos la info bonita
            mensaje = f"[{time.strftime('%H:%M:%S')}] En {ciudad}: {temperatura}ºC ({descripcion})"
            
            # Decisión lógica (simulada)
            if temperatura < 0:
                print(f"{mensaje} -> ¡ALERTA FRIO! (Se pararía la carga)")
            elif temperatura > 45:
                print(f"{mensaje} -> ¡ALERTA CALOR! (Peligro)")
            else:
                print(f"{mensaje} -> OK (Operativo)")
        
        # Esperamos 5 segundos
        time.sleep(5)

if __name__ == "__main__":
    main()
