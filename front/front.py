#!/usr/bin/env python3
"""
Servidor web simple para el frontend del EV Charging Network
Sirve el dashboard en http://localhost:8080
"""

import http.server
import socketserver
import os
import sys

PORT = 8080
DIRECTORY = "."

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Handler con CORS habilitado para permitir peticiones al API"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)
    
    def end_headers(self):
        # Agregar headers CORS
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
    
    def log_message(self, format, *args):
        # Personalizar logs
        print(f"[WEB] {self.address_string()} - {format % args}")


def main():
    # Verificar que index.html existe
    if not os.path.exists('index.html'):
        print("=" * 60)
        print("❌ ERROR: No se encuentra 'index.html'")
        print("=" * 60)
        print("Asegúrate de que el archivo index.html está en el mismo")
        print("directorio que este script.")
        print("=" * 60)
        sys.exit(1)
    
    try:
        with socketserver.TCPServer(("", PORT), CORSRequestHandler) as httpd:
            print("=" * 60)
            print("🌐 Servidor Web EV Charging Network")
            print("=" * 60)
            print(f"✓ Servidor iniciado en http://localhost:{PORT}")
            print(f"✓ Dashboard disponible en http://localhost:{PORT}/index.html")
            print("=" * 60)
            print("")
            print("📋 INSTRUCCIONES:")
            print("1. Asegúrate de que EV_Central está corriendo")
            print("   (para que el API esté disponible en puerto 5000)")
            print("")
            print("2. Abre tu navegador en:")
            print(f"   http://localhost:{PORT}/index.html")
            print("")
            print("3. El dashboard se actualizará automáticamente cada 3 segundos")
            print("")
            print("=" * 60)
            print("Presiona Ctrl+C para detener el servidor")
            print("=" * 60)
            print("")
            
            httpd.serve_forever()
            
    except KeyboardInterrupt:
        print("\n\n[WEB] Servidor detenido por el usuario.")
        sys.exit(0)
    except OSError as e:
        if "Address already in use" in str(e):
            print("=" * 60)
            print(f"❌ ERROR: El puerto {PORT} ya está en uso")
            print("=" * 60)
            print("Soluciones:")
            print(f"1. Cierra cualquier aplicación usando el puerto {PORT}")
            print("2. O modifica la variable PORT en este script")
            print("=" * 60)
            sys.exit(1)
        else:
            raise


if __name__ == "__main__":
    main()