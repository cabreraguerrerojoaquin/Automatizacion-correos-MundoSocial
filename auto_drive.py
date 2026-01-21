import os
import smtplib
import datetime
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
import io

# --- CONFIGURACIÓN ---
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

FOLDER_INBOX_ID = os.environ.get('INPUT_ID')
FOLDER_PROCESSED_ID = os.environ.get('OUTPUT_ID')
SHEET_ID = os.environ.get('SHEET_ID')
SMTP_USER = os.environ.get('GMAIL_USER')
SMTP_PASS = os.environ.get('GMAIL_PASS')

# Autenticación
creds_json = json.loads(os.environ.get('GCP_SA_KEY'))
creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)
gc = gspread.authorize(creds)

def enviar_correo(destinatarios, asunto, cuerpo, archivo_adjunto=None, nombre_archivo=None):
    """
    Envía correos a uno o varios destinatarios.
    Acepta 'destinatarios' como string (uno solo) o list (varios).
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['Subject'] = asunto
        msg.attach(MIMEText(cuerpo, 'plain'))

        # Manejo de lista de correos
        if isinstance(destinatarios, list):
            msg['To'] = ", ".join(destinatarios)
            lista_envio = destinatarios
        else:
            msg['To'] = destinatarios
            lista_envio = [destinatarios]

        # Adjuntar archivo si existe
        if archivo_adjunto:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(archivo_adjunto.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{nombre_archivo}"')
            msg.attach(part)
            # Resetear el puntero del archivo por si se necesitara reusar (buena práctica)
            archivo_adjunto.seek(0)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        
        # Enviar a la lista
        server.sendmail(SMTP_USER, lista_envio, msg.as_string())
        server.quit()
        print(f"📧 Correo enviado a: {msg['To']}")
        
    except Exception as e:
        print(f"❌ Error enviando correo: {e}")

def descargar_archivo(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh

def mover_archivo(file_id):
    try:
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        prev_parents = ",".join(file.get('parents'))
        drive_service.files().update(
            fileId=file_id,
            addParents=FOLDER_PROCESSED_ID,
            removeParents=prev_parents,
            fields='id, parents'
        ).execute()
        print(f"🚚 Archivo {file_id} procesado y movido.")
    except Exception as e:
        print(f"⚠️ Error moviendo archivo: {e}")

def procesar_nuevas_entregas(sheet):
    print("--- 🔍 Buscando entregas nuevas ---")
    
    results = drive_service.files().list(
        q=f"'{FOLDER_INBOX_ID}' in parents and trashed=false and mimeType != 'application/vnd.google-apps.folder'",
        fields="files(id, name, owners(emailAddress))"
    ).execute()
    items = results.get('files', [])

    if not items:
        print("✅ No hay archivos pendientes.")
        return

    records = sheet.get_all_records()
    
    for item in items:
        file_id = item['id']
        file_name = item['name']
        try:
            owner_email = item['owners'][0]['emailAddress']
        except:
            continue

        print(f"📂 Procesando entrega de: {owner_email}")

        # Buscar quién subió el archivo
        uploader_data = None
        uploader_row = None
        
        for i, record in enumerate(records):
            if record['Email Integrante'] == owner_email:
                uploader_row = i + 2
                uploader_data = record
                break
        
        if uploader_data:
            # 1. Actualizar BD
            fecha_hoy = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(uploader_row, 5, "Entregado") 
            sheet.update_cell(uploader_row, 6, fecha_hoy)
            print(f"✅ Estado actualizado para {uploader_data['Nombre Integrante']}")

            # 2. Descargar archivo
            archivo_memoria = descargar_archivo(file_id)

            # --- NUEVA LÓGICA DE GRUPO ---
            grupo_id = uploader_data['Grupo']
            
            # Filtramos a TODOS los miembros de este grupo
            # Buscamos en 'records' todos los que tengan el mismo Grupo
            emails_grupo = [r['Email Integrante'] for r in records if r['Grupo'] == grupo_id and r['Email Integrante']]
            
            # Eliminamos duplicados por seguridad
            emails_grupo = list(set(emails_grupo))
            
            print(f"👥 Notificando al grupo {grupo_id}: {emails_grupo}")

            # 3. Enviar correo a TODO EL GRUPO
            asunto = f"Entrega Exitosa - Grupo {grupo_id}"
            cuerpo = (
                f"Hola equipo,\n\n"
                f"El integrante {uploader_data['Nombre Integrante']} ha subido un nuevo avance.\n"
                f"Archivo: {file_name}\n"
                f"Fecha: {fecha_hoy}\n\n"
                f"Adjuntamos copia del archivo para su revisión.\n"
            )
            
            # Enviamos un solo correo con copia a todos
            enviar_correo(emails_grupo, asunto, cuerpo, archivo_memoria, file_name)

            # 4. Mover archivo
            mover_archivo(file_id)
        else:
            print(f"⚠️ Correo no registrado: {owner_email}")

def reporte_semanal(sheet):
    """
    Reporte de Sábado. 
    Ahora notifica a todo el grupo si cumplieron o no.
    """
    print("--- 📊 Generando Reporte Semanal ---")
    records = sheet.get_all_records()
    
    grupos_status = {} # Diccionario para agrupar estados por grupo

    # Analizar estados
    for r in records:
        grupo = r['Grupo']
        email = r['Email Integrante']
        
        if grupo not in grupos_status:
            grupos_status[grupo] = {'emails': [], 'entregado': False}
        
        grupos_status[grupo]['emails'].append(email)
        
        # Si AL MENOS UNO del grupo entregó, consideramos al grupo como cumplido
        # (Puedes cambiar esta lógica si TODOS deben entregar)
        if r['Estado'] == 'Entregado':
            grupos_status[grupo]['entregado'] = True

    # Enviar reportes
    cuerpo_rrhh = "Resumen Semanal:\n"
    
    for grupo, datos in grupos_status.items():
        emails = datos['emails']
        
        if datos['entregado']:
            # Grupo Cumplido
            enviar_correo(emails, "Reporte Semanal: ✅ Objetivo Cumplido", 
                          f"Felicidades Grupo {grupo}, hemos registrado su avance esta semana.")
            cuerpo_rrhh += f"- Grupo {grupo}: CUMPLIÓ\n"
        else:
            # Grupo Incumplido
            enviar_correo(emails, "Reporte Semanal: ❌ Falta Entrega", 
                          f"Estimado Grupo {grupo}, no se registró ninguna entrega esta semana. Por favor regularizar.")
            cuerpo_rrhh += f"- Grupo {grupo}: NO ENTREGÓ\n"

    # Reporte a RRHH
    enviar_correo("recursoshumanos@gmail.com", "Informe de Avances Semanal", cuerpo_rrhh)
    print("✅ Reporte semanal finalizado.")

def main():
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.sheet1
    
    procesar_nuevas_entregas(worksheet)

    if datetime.datetime.today().weekday() == 5:
        reporte_semanal(worksheet)

if __name__ == '__main__':
    main()
