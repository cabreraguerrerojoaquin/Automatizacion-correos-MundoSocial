import os
import json
import smtplib
import datetime
import time
import random
import sys
import argparse
import logging
import io
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# ==========================================
# 0. CONFIGURACIÓN DE LOGS ESTRUCTURADOS
# ==========================================
# Esto configura cómo se verán los mensajes en la consola de GitHub Actions
logging.basicConfig(
    level=logging.INFO, # Solo mostrar INFO, WARNING y ERROR (Ocultar DEBUG)
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S', # Formato de hora limpio
    handlers=[
        logging.StreamHandler(sys.stdout) # Asegura que salga en la consola de GitHub
    ]
)
logger = logging.getLogger()

# ==========================================
# 1. CARGA DE VARIABLES
# ==========================================
def get_env_var(name):
    val = os.environ.get(name)
    if not val:
        logger.critical(f"Error Crítico: La variable de entorno '{name}' no existe.")
        sys.exit(1) # Detener ejecución con código de error
    return val

logger.info("---INICIANDO CONFIGURACIÓN ---")

try:
    FOLDER_INBOX_ID = get_env_var('DRIVE_INBOX_ID')
    FOLDER_PROCESSED_ID = get_env_var('DRIVE_PROCESSED_ID')
    SHEET_ID = get_env_var('SHEET_ID')
    GMAIL_USER = get_env_var('GMAIL_USER')
    GMAIL_PASS = get_env_var('GMAIL_PASS')
    GCP_KEY_JSON = json.loads(get_env_var('GCP_SA_KEY'))
    logger.info("Variables de entorno cargadas correctamente.")
except Exception as e:
    logger.critical(f"Error cargando configuración: {e}")
    sys.exit(1)

# CORREOS DE AUTORIDADES
EMAIL_RRHH = ["recursoshumanos@ejemplo.com"] 
EMAIL_DIRECTIVA = ["presidente@ejemplo.com", "secretario@ejemplo.com"]

# ==========================================
# 2. CONEXIÓN GOOGLE
# ==========================================
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

try:
    creds = Credentials.from_service_account_info(GCP_KEY_JSON, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    gc = gspread.authorize(creds)
    logger.info("Conexión con Google API establecida.")
except Exception as e:
    logger.critical(f"Fallo en conexión con Google: {e}")
    sys.exit(1)

# ==========================================
# 3. UTILIDADES
# ==========================================
def obtener_hora_peru():
    """Hora UTC -5"""
    hora_utc = datetime.datetime.utcnow()
    hora_peru = hora_utc - datetime.timedelta(hours=5)
    return hora_peru.strftime("%Y-%m-%d %H:%M:%S (Hora Perú)")

def enviar_correo_individual(destinatario, asunto, cuerpo, archivo_memoria=None, nombre_archivo=None):
    try:
        msg = MIMEMultipart()
        msg['From'] = GMAIL_USER
        msg['To'] = destinatario
        msg['Subject'] = asunto
        msg.attach(MIMEText(cuerpo, 'plain'))

        if archivo_memoria:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(archivo_memoria.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{nombre_archivo}"')
            msg.attach(part)
            archivo_memoria.seek(0)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_PASS)
        server.sendmail(GMAIL_USER, destinatario, msg.as_string())
        server.quit()
        logger.info(f"Enviado correctamente a: {destinatario}")
        return True
    except Exception as e:
        logger.error(f"Error envío a {destinatario}: {e}")
        return False

def mover_archivo(file_id):
    try:
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        prev_parents = ",".join(file.get('parents'))
        drive_service.files().update(fileId=file_id, addParents=FOLDER_PROCESSED_ID, removeParents=prev_parents).execute()
        logger.info(f"Archivo movido a carpeta Procesados.")
    except Exception as e: 
        logger.warning(f"No se pudo mover el archivo: {e}")

def descargar_archivo_ram(file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except Exception as e:
        logger.error(f"Error descargando archivo: {e}")
        return None

# ==========================================
# 4. LÓGICA DE NEGOCIO
# ==========================================

def procesar_nuevas_entregas():
    logger.info("--- MODO: VERIFICACIÓN DE ENTREGAS ---")
    
    try:
        results = drive_service.files().list(
            q=f"'{FOLDER_INBOX_ID}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'",
            fields="files(id, name, owners(emailAddress))"
        ).execute()
        items = results.get('files', [])
    except Exception as e:
        logger.error(f"Error consultando Drive: {e}")
        return

    if not items:
        logger.info("No se encontraron archivos nuevos. Finalizando.")
        return

    logger.info(f"Se encontraron {len(items)} archivos nuevos.")
    
    # Cargar BD
    try:
        sh = gc.open_by_key(SHEET_ID)
        worksheet = sh.sheet1
        records = worksheet.get_all_records()
    except Exception as e:
        logger.critical(f"Error leyendo Excel: {e}")
        return

    for item in items:
        file_id = item['id']
        file_name = item['name']
        try: owner_email = item['owners'][0]['emailAddress']
        except: 
            logger.warning(f"Archivo {file_name} sin dueño identificable.")
            continue

        logger.info(f"Procesando archivo: {file_name} (De: {owner_email})")

        grupo_identificado = None
        nombre_subidor = "Desconocido"
        
        # Identificar usuario
        for r in records:
            if r.get('Email Integrante') == owner_email:
                grupo_identificado = str(r.get('Grupo'))
                nombre_subidor = r.get('Nombre Integrante')
                break
        
        if grupo_identificado:
            logger.info(f"Grupo detectado: {grupo_identificado}")
            fecha_hora_peru = obtener_hora_peru()
            emails_grupo = []
            
            # Actualizar Excel
            for i, r in enumerate(records):
                if str(r.get('Grupo')) == grupo_identificado:
                    if r.get('Email Integrante'):
                        emails_grupo.append(r['Email Integrante'])
                    
                    if r.get('Estado') != "Entregado":
                        try:
                            worksheet.update_cell(i + 2, 4, "Entregado")
                            worksheet.update_cell(i + 2, 5, fecha_hora_peru)
                            logger.info(f"Excel actualizado para: {r.get('Nombre Integrante')}")
                            time.sleep(1.5) 
                        except Exception as e:
                            logger.error(f"Error escribiendo en Excel: {e}")
            
            # Preparar destinatarios
            destinatarios = emails_grupo + EMAIL_DIRECTIVA
            destinatarios = list(set([d.strip() for d in destinatarios if d and "@" in d]))
            
            archivo_ram = descargar_archivo_ram(file_id)
            if not archivo_ram: continue # Si falló descarga, saltar

            asunto = f"Entrega Exitosa - Grupo {grupo_identificado}"
            cuerpo = (f"Se confirma recepción del archivo de {nombre_subidor}.\n"
                      f"Grupo: {grupo_identificado}\n"
                      f"Fecha: {fecha_hora_peru}\n\n"
                      f"Copia automática a Junta Directiva.")

            logger.info(f"Iniciando envíos a {len(destinatarios)} personas...")
            enviado_al_menos_uno = False
            for dest in destinatarios:
                pausa = random.randint(5, 10)
                time.sleep(pausa) 
                if enviar_correo_individual(dest, asunto, cuerpo, archivo_ram, file_name):
                    enviado_al_menos_uno = True

            if enviado_al_menos_uno:
                mover_archivo(file_id)
            else:
                logger.error("Fallaron todos los envíos de correo. No se mueve el archivo.")
        else:
            logger.warning(f"Correo {owner_email} no encontrado en la base de datos.")

def reporte_semanal_pendientes():
    logger.info("--- MODO: REPORTE SEMANAL (SÁBADO) ---")
    try:
        sh = gc.open_by_key(SHEET_ID)
        records = sh.sheet1.get_all_records()
    except Exception as e:
        logger.critical(f"Error acceso Excel: {e}")
        return

    grupos_pendientes = {}

    for r in records:
        if str(r.get('Estado')).strip().lower() != "entregado":
            grupo = str(r.get('Grupo'))
            if grupo not in grupos_pendientes: grupos_pendientes[grupo] = []
            if r.get('Email Integrante'): grupos_pendientes[grupo].append(r['Email Integrante'])

    if not grupos_pendientes:
        logger.info("Todos han cumplido esta semana. No hay reportes.")
        return

    logger.info(f" Detectados {len(grupos_pendientes)} grupos pendientes.")
    hora = obtener_hora_peru()

    for grupo, miembros in grupos_pendientes.items():
        destinatarios = miembros + EMAIL_RRHH + EMAIL_DIRECTIVA
        destinatarios = list(set([d.strip() for d in destinatarios if d and "@" in d]))
        
        asunto = f"ALERTA INCUMPLIMIENTO - Grupo {grupo}"
        cuerpo = f"Atención Grupo {grupo}: No se registró entrega al cierre de semana ({hora}).\nCopia a Directiva/RRHH."

        logger.info(f"Notificando Grupo {grupo} ({len(destinatarios)} destinatarios)...")
        for dest in destinatarios:
            time.sleep(random.randint(5, 10))
            enviar_correo_individual(dest, asunto, cuerpo)

# ==========================================
# 5. PUNTO DE ENTRADA
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--modo", choices=["verificar", "reporte"], required=True, help="Modo de ejecución")
    args = parser.parse_args()

    start_time = time.time()
    
    if args.modo == "verificar":
        procesar_nuevas_entregas()
    elif args.modo == "reporte":
        reporte_semanal_pendientes()
    
    duration = time.time() - start_time
    logger.info(f"🏁 Ejecución finalizada en {duration:.2f} segundos.")
