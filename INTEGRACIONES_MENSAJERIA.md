# Integraciones de Mensajería - WhatsApp y Telegram

## Índice
1. [Descripción General](#descripción-general)
2. [Arquitectura](#arquitectura)
3. [Configuración de WhatsApp](#configuración-de-whatsapp)
4. [Configuración de Telegram](#configuración-de-telegram)
5. [Uso de las APIs](#uso-de-las-apis)
6. [Ejemplos de Uso](#ejemplos-de-uso)
7. [Webhook vs Polling](#webhook-vs-polling)
8. [Limitaciones y Consideraciones](#limitaciones-y-consideraciones)
9. [Troubleshooting](#troubleshooting)

---

## Descripción General

TransformAR ahora soporta la lectura de mensajes y archivos adjuntos desde **WhatsApp Business** y **Telegram**, permitiendo el mismo flujo de procesamiento y transformación de datos que las integraciones existentes de Gmail y Outlook.

### Características Principales

- ✅ **Lectura de mensajes** de texto
- ✅ **Descarga de archivos adjuntos** (documentos, imágenes, videos, audio)
- ✅ **Procesamiento con IA** usando el mismo pipeline que correos
- ✅ **Webhooks en tiempo real** para recibir mensajes automáticamente
- ✅ **APIs gratuitas** sin costo adicional
- ✅ **Respuestas automáticas** (opcional)

### Comparación de Integraciones

| Característica | Gmail | Outlook | WhatsApp | Telegram |
|----------------|-------|---------|----------|----------|
| **Método de Auth** | OAuth 2.0 | OAuth 2.0 | Token fijo | Bot Token |
| **Costo** | Gratis | Gratis | Gratis (1000 msg/día) | Gratis (sin límite) |
| **Webhook** | No | No | Sí (recomendado) | Sí (opcional) |
| **Adjuntos** | Sí | Sí | Sí (4 tipos) | Sí (12+ tipos) |
| **Respuestas automáticas** | No | No | Sí | Sí |
| **Adopción empresarial** | Alta | Alta | Muy Alta | Media |
| **Facilidad de setup** | Media | Media | Media | Fácil |

---

## Arquitectura

Ambas integraciones siguen el **mismo patrón** que Gmail y Outlook:

```
Mensaje entrante
    ↓
[authenticate_*] → Autenticación
    ↓
[list_messages_*] → Listar conversaciones/mensajes
    ↓
[get_message_content] → Obtener contenido + descargar adjuntos
    ↓
[extract_text_with_layout] → Extraer texto de PDF/imágenes (Docling + OCR)
    ↓
[extract_with_qwen] → Extraer datos estructurados con IA
    ↓
[interpret_with_qwen] → Interpretar instrucciones de transformación
    ↓
[execute_plan] → Ejecutar transformaciones
    ↓
Resultado JSON
```

### Archivos Nuevos

- `input/whatsapp_reader.py` - Cliente y funciones para WhatsApp Business API
- `input/telegram_reader.py` - Cliente y funciones para Telegram Bot API
- `api.py` (modificado) - Nuevos endpoints y DTOs

---

## Configuración de WhatsApp

### Paso 1: Crear Aplicación en Meta for Business

1. Ve a [Meta for Business](https://business.facebook.com/)
2. Crea un **Workspace** si no tienes uno
3. Crea una nueva **App** y selecciona **WhatsApp**
4. En **Settings > WhatsApp > API Setup**, encontrarás:
   - **Phone Number ID**
   - **Access Token** (temporal)
   - **Webhook Token** (lo defines tú)

### Paso 2: Obtener Token Permanente

El token temporal expira en 24h. Para obtener uno permanente:

```bash
# Método 1: Usar System User Token (recomendado)
# 1. En Meta for Business, ve a Settings > System Users
# 2. Crea un System User
# 3. Asigna permisos de WhatsApp
# 4. Genera un token permanente

# Método 2: Usar Graph API Explorer
# Visita: https://developers.facebook.com/tools/explorer/
# Selecciona tu app, genera token con permisos whatsapp_business_management
```

### Paso 3: Configurar Webhook

El webhook permite recibir mensajes en tiempo real.

1. **En Meta for Developers:**
   - Ve a **WhatsApp > Configuration**
   - Callback URL: `https://tu-dominio.com/webhook/whatsapp`
   - Verify Token: un string aleatorio (guárdalo en `.env`)
   - Suscríbete a: `messages`

2. **Importante:** El webhook **DEBE usar HTTPS** (no funciona con HTTP)

### Paso 4: Variables de Entorno

Copia estas variables en tu `.env`:

```bash
WHATSAPP_PHONE_NUMBER_ID=123456789012345
WHATSAPP_ACCESS_TOKEN=EAAQ...your-token
WHATSAPP_WEBHOOK_TOKEN=mi_token_secreto_xyz123
```

### Paso 5: Verificar Configuración

```bash
# Ejecutar el test del módulo
python input/whatsapp_reader.py

# Deberías ver:
# ✓ Cliente WhatsApp inicializado correctamente
```

---

## Configuración de Telegram

### Paso 1: Crear Bot con BotFather

1. Abre Telegram y busca **@BotFather**
2. Envía el comando `/newbot`
3. Elige un **nombre** para tu bot (ej: "TransformAR Bot")
4. Elige un **username** que termine en "bot" (ej: `transformar_bot`)
5. Copia el **token** que te da BotFather

**Ejemplo:**
```
BotFather: Done! Congratulations on your new bot.
You will find it at t.me/transformar_bot
Use this token to access the HTTP API:
123456789:ABCDEFGHijklmnopQRSTUVWXYZ-1234567890
```

### Paso 2: Variables de Entorno

Agrega a tu `.env`:

```bash
TELEGRAM_BOT_TOKEN=123456789:ABCDEFGHijklmnopQRSTUVWXYZ-1234567890
```

### Paso 3: (Opcional) Configurar Webhook

Por defecto, Telegram usa **polling** (consultar periódicamente). Para webhooks:

```bash
# Configurar webhook
curl -X POST https://api.telegram.org/bot<TU_TOKEN>/setWebhook \
  -H "Content-Type: application/json" \
  -d '{"url": "https://tu-dominio.com/webhook/telegram"}'

# Verificar webhook
curl https://api.telegram.org/bot<TU_TOKEN>/getWebhookInfo
```

### Paso 4: Verificar Configuración

```bash
# Ejecutar el test del módulo
python input/telegram_reader.py

# Deberías ver:
# ✓ Cliente Telegram inicializado correctamente
# Obteniendo últimos mensajes...
```

---

## Uso de las APIs

### Endpoints Disponibles

#### WhatsApp

```http
# Listar mensajes (placeholder, requiere implementar webhook + DB)
GET /input/whatsapp/messages?limit=10

# Obtener contenido de un mensaje
POST /input/whatsapp/content
Body: {
  "type": "text",
  "text": {"body": "Hola"},
  ...
}

# Webhook para recibir mensajes
POST /webhook/whatsapp
GET /webhook/whatsapp  # Verificación

# Procesar mensaje
POST /process
Body: {
  "method": "whatsapp",
  "template_id": "plantilla-1",
  "whatsapp": {
    "message_data": { ... },
    "use_text": false,
    "attachment_index": 0
  }
}
```

#### Telegram

```http
# Listar mensajes (polling)
GET /input/telegram/messages?limit=10

# Obtener contenido de un mensaje
POST /input/telegram/content
Body: {
  "message_id": "123",
  "chat_id": 987654321,
  "text": "Hola",
  ...
}

# Webhook para recibir mensajes
POST /webhook/telegram

# Procesar mensaje
POST /process
Body: {
  "method": "telegram",
  "template_id": "plantilla-1",
  "telegram": {
    "message_data": { ... },
    "use_text": false,
    "attachment_index": 0
  }
}
```

---

## Ejemplos de Uso

### Ejemplo 1: Procesar Documento de WhatsApp

```python
import requests

# 1. Usuario envía documento por WhatsApp
# 2. El webhook recibe el mensaje y lo guarda en DB

# 3. Frontend obtiene el mensaje
message_data = {
    "type": "document",
    "document": {
        "id": "media-id-123",
        "filename": "pedido.pdf"
    },
    "from": "+5491112345678"
}

# 4. Procesar con TransformAR
response = requests.post("http://localhost:8000/process", json={
    "method": "whatsapp",
    "template_id": "plantilla-pedidos",
    "whatsapp": {
        "message_data": message_data,
        "use_text": False,
        "attachment_index": 0
    }
})

result = response.json()
print(result["result"])  # Datos extraídos y transformados
```

### Ejemplo 2: Leer Mensajes de Telegram

```python
import requests

# Obtener últimos mensajes
response = requests.get("http://localhost:8000/input/telegram/messages?limit=5")
messages = response.json()["messages"]

for msg in messages:
    print(f"{msg['from']['username']}: {msg['text']}")

    # Si tiene documento adjunto, procesarlo
    if msg['type'] == 'document':
        process_response = requests.post("http://localhost:8000/process", json={
            "method": "telegram",
            "template_id": "plantilla-facturas",
            "telegram": {
                "message_data": msg,
                "use_text": False,
                "attachment_index": 0
            }
        })
        print(process_response.json())
```

### Ejemplo 3: Enviar Respuesta Automática

```python
from input.whatsapp_reader import authenticate_whatsapp

client = authenticate_whatsapp()

# Enviar mensaje de texto
client.send_text_message(
    to_phone="+5491112345678",
    message="✅ Tu pedido fue procesado correctamente"
)

# Para Telegram
from input.telegram_reader import authenticate_telegram

telegram_client = authenticate_telegram()
telegram_client.send_message(
    chat_id=987654321,
    text="<b>✅ Documento procesado</b>",
    parse_mode="HTML"
)
```

---

## Webhook vs Polling

### WhatsApp

| Método | Descripción | Recomendación |
|--------|-------------|---------------|
| **Webhook** | Meta envía mensajes a tu servidor en tiempo real | ✅ **Recomendado para producción** |
| **Polling** | No disponible directamente en WhatsApp Business API | ❌ No soportado |

**Nota:** WhatsApp requiere webhook obligatorio para recibir mensajes.

### Telegram

| Método | Descripción | Recomendación |
|--------|-------------|---------------|
| **Webhook** | Telegram envía mensajes a tu servidor en tiempo real | ✅ **Recomendado para producción** |
| **Polling** | Tu servidor consulta periódicamente si hay mensajes nuevos | ⚠️ **Útil para desarrollo/testing** |

**Ventajas del Webhook:**
- Respuesta instantánea
- Menor uso de recursos
- Escalable

**Ventajas del Polling:**
- No requiere dominio público
- Más fácil de testear localmente
- No requiere HTTPS

---

## Limitaciones y Consideraciones

### WhatsApp Business API

| Limitación | Detalles |
|------------|----------|
| **Mensajes gratuitos** | 1000 conversaciones por mes (gratis) |
| **Verificación de negocio** | Requerido para aumentar límites |
| **Requisitos de webhook** | HTTPS obligatorio, respuesta en <20s |
| **Rate limiting** | Varía según plan y verificación |
| **Listado de mensajes** | No hay endpoint para listar históricos, solo webhook |

### Telegram Bot API

| Limitación | Detalles |
|------------|----------|
| **Rate limiting** | 30 mensajes/segundo por bot |
| **Tamaño de archivos** | Máximo 20MB por archivo (50MB para bots premium) |
| **Requisitos de webhook** | HTTPS obligatorio si se usa webhook |
| **Privacidad** | Los bots NO pueden iniciar conversaciones con usuarios |

### Ambas Plataformas

- **Almacenamiento:** Los mensajes NO se almacenan automáticamente, debes implementar persistencia en base de datos
- **HTTPS:** Obligatorio para webhooks en producción
- **Respuesta rápida:** Los webhooks deben responder en <20 segundos
- **Seguridad:** Valida siempre las firmas/tokens de los webhooks

---

## Troubleshooting

### WhatsApp

**Problema:** "Error de configuración: Faltan variables de entorno"
```bash
# Solución: Verificar que .env contenga:
WHATSAPP_PHONE_NUMBER_ID=...
WHATSAPP_ACCESS_TOKEN=...
```

**Problema:** "Webhook verification failed"
```bash
# Solución:
# 1. Verificar que WHATSAPP_WEBHOOK_TOKEN coincida con el configurado en Meta
# 2. Asegurarse de que el endpoint GET /webhook/whatsapp esté funcionando
# 3. Verificar logs del servidor
```

**Problema:** "Failed to download media"
```bash
# Posibles causas:
# 1. Token expirado o inválido
# 2. Media ID incorrecto
# 3. Archivo eliminado de los servidores de WhatsApp (expiran en 30 días)
```

### Telegram

**Problema:** "No hay mensajes nuevos"
```bash
# Posibles causas:
# 1. El bot no ha recibido mensajes aún (enviarle uno directamente)
# 2. Offset incorrecto (borrar caché con: client.offset = None)
# 3. Bot bloqueado por el usuario
```

**Problema:** "Error 401: Unauthorized"
```bash
# Solución:
# 1. Verificar que TELEGRAM_BOT_TOKEN sea correcto
# 2. Copiar token completo desde @BotFather (incluye números:letras)
```

**Problema:** "Webhook already set"
```bash
# Solución: Eliminar webhook existente
curl https://api.telegram.org/bot<TOKEN>/deleteWebhook
```

### General

**Problema:** "HTTPS certificate invalid"
```bash
# Webhooks requieren certificado SSL válido
# Opciones:
# 1. Usar servicio con HTTPS (Heroku, AWS, DigitalOcean)
# 2. Usar ngrok para testing: ngrok http 8000
# 3. Obtener certificado gratis con Let's Encrypt
```

**Problema:** "Archivos no se descargan"
```bash
# Verificar permisos de carpeta attachments/
mkdir -p attachments
chmod 755 attachments
```

---

## Recursos Adicionales

### Documentación Oficial

- [WhatsApp Business API Docs](https://developers.facebook.com/docs/whatsapp/cloud-api)
- [Telegram Bot API Docs](https://core.telegram.org/bots/api)
- [Meta for Developers](https://developers.facebook.com/)

### Herramientas Útiles

- **Postman Collection:** Para probar endpoints
- **ngrok:** Para exponer servidor local con HTTPS
- **Graph API Explorer:** Para probar llamadas a WhatsApp API

### Próximos Pasos

1. ✅ Implementar persistencia de mensajes en Firestore
2. ⏳ Agregar soporte para mensajes de voz
3. ⏳ Implementar conversaciones multi-turno
4. ⏳ Agregar métricas y analytics
5. ⏳ Soporte para stickers y reacciones

---

## Soporte

Para preguntas o problemas:
- Revisar logs del servidor: `uvicorn api:app --reload`
- Verificar variables de entorno
- Consultar documentación oficial de cada plataforma
- Abrir issue en el repositorio

---

**Actualizado:** 2025-10-31
**Versión:** 1.0.0
