# Integraciones Multi-Tenant por Usuario

## 📋 Índice

1. [Descripción General](#descripción-general)
2. [Arquitectura](#arquitectura)
3. [Endpoints Disponibles](#endpoints-disponibles)
4. [Flujos de Conexión](#flujos-de-conexión)
5. [Ejemplos de Uso](#ejemplos-de-uso)
6. [Seguridad y Almacenamiento](#seguridad-y-almacenamiento)
7. [Frontend Integration](#frontend-integration)
8. [Troubleshooting](#troubleshooting)

---

## Descripción General

TransformAR ahora soporta **integraciones multi-tenant**, donde cada usuario puede conectar **sus propias cuentas** de:

- ✅ **Gmail** (OAuth 2.0)
- ✅ **Outlook** (OAuth 2.0)
- ✅ **WhatsApp Business** (API credentials)
- ✅ **Telegram Bot** (Bot token)

### Cambios Importantes

| Antes (v1) | Ahora (v2) |
|------------|------------|
| ❌ Credenciales globales en `.env` | ✅ Credenciales por usuario en Firestore |
| ❌ Una sola cuenta por servicio | ✅ Cada usuario su propia cuenta |
| ❌ Inseguro (todos comparten tokens) | ✅ Seguro (tokens aislados por usuario) |
| ❌ No escalable | ✅ Totalmente escalable |

---

## Arquitectura

### Estructura de Datos en Firestore

```
users/
  {userId}/
    external_credentials/
      gmail/
        service: "gmail"
        connected_at: timestamp
        credentials:
          token: "..."
          refresh_token: "..."
          client_id: "..."
          client_secret: "..."
          scopes: [...]
        metadata:
          email: "user@gmail.com"
          provider: "gmail"
        updated_at: timestamp

      outlook/
        service: "outlook"
        credentials:
          access_token: "..."
          refresh_token: "..."
          expires_at: 1234567890
        metadata:
          email: "user@outlook.com"

      whatsapp/
        service: "whatsapp"
        credentials:
          phone_number_id: "..."
          access_token: "..."
          business_account_id: "..."
        metadata:
          phone_number: "+5491112345678"

      telegram/
        service: "telegram"
        credentials:
          bot_token: "..."
          bot_username: "my_bot"
        metadata:
          bot_name: "My Bot"
          bot_username: "my_bot"
```

### Flujo General

```
Usuario Frontend
    ↓
[1] Autenticación (POST /auth/login)
    → Recibe JWT token
    ↓
[2] Conectar Servicio (POST /integrations/{service}/connect)
    → Gmail/Outlook: Redirect a OAuth
    → WhatsApp/Telegram: Enviar credenciales directamente
    ↓
[3] Callback OAuth (GET /integrations/{service}/callback)
    → Guarda credenciales en Firestore
    → Redirect al frontend
    ↓
[4] Usar Integración (POST /process)
    → Sistema obtiene credenciales del usuario automáticamente
    → Procesa mensajes/correos con esas credenciales
```

---

## Endpoints Disponibles

### Autenticación (Prerequisito)

Todos los endpoints de integración requieren un **token JWT** en el header:

```http
Authorization: Bearer {jwt_token}
```

### Listar Integraciones

```http
GET /integrations/
Authorization: Bearer {token}

Response:
{
  "integrations": {
    "gmail": {
      "service": "gmail",
      "connected_at": "2025-01-15T10:30:00Z",
      "metadata": {
        "email": "user@gmail.com",
        "provider": "gmail"
      }
    },
    "telegram": {
      "service": "telegram",
      "connected_at": "2025-01-15T11:00:00Z",
      "metadata": {
        "bot_username": "my_bot",
        "bot_name": "My Bot"
      }
    }
  }
}
```

---

## Flujos de Conexión

### 1. Gmail (OAuth 2.0)

**Paso 1: Iniciar conexión**

```http
POST /integrations/gmail/connect
Authorization: Bearer {token}

Response:
{
  "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth?...",
  "message": "Redirige al usuario a esta URL para autorizar el acceso"
}
```

**Paso 2: Usuario autoriza en Google**

El frontend redirige al usuario a `authorization_url`. El usuario:
1. Inicia sesión en Google (si no lo está)
2. Autoriza el acceso a su Gmail
3. Es redirigido a: `http://localhost:8000/integrations/gmail/callback?code=...&state=...`

**Paso 3: Callback automático**

```http
GET /integrations/gmail/callback?code=xxx&state=yyy

Response:
{
  "success": true,
  "message": "Gmail conectado exitosamente: user@gmail.com",
  "redirect": "http://localhost:3000/integrations?success=gmail"
}
```

El backend:
- Intercambia el código por tokens de acceso
- Guarda tokens en Firestore
- Redirige al frontend con mensaje de éxito

**Paso 4: Desconectar**

```http
DELETE /integrations/gmail/disconnect
Authorization: Bearer {token}

Response:
{
  "success": true,
  "message": "Gmail desconectado exitosamente",
  "service": "gmail"
}
```

---

### 2. Outlook (OAuth 2.0)

**Flujo idéntico a Gmail**, pero con endpoints de Outlook:

```http
POST /integrations/outlook/connect
→ Retorna authorization_url de Microsoft

GET /integrations/outlook/callback?code=...&state=...
→ Guarda tokens y redirige al frontend

DELETE /integrations/outlook/disconnect
→ Elimina credenciales
```

**Diferencias:**
- Usa Microsoft Graph API en lugar de Google APIs
- Requiere `OUTLOOK_CLIENT_ID` y `OUTLOOK_CLIENT_SECRET` en `.env`

---

### 3. WhatsApp Business

**Paso 1: Obtener credenciales desde Meta**

El usuario debe:
1. Ir a https://business.facebook.com
2. Crear/seleccionar workspace
3. Agregar aplicación WhatsApp
4. Copiar desde Settings > API Setup:
   - Phone Number ID
   - Access Token
   - Business Account ID (opcional)

**Paso 2: Conectar**

```http
POST /integrations/whatsapp/connect
Authorization: Bearer {token}
Content-Type: application/json

{
  "phone_number_id": "123456789012345",
  "access_token": "EAAQ...long-token",
  "business_account_id": "123456789012345",
  "phone_number": "+5491112345678"
}

Response:
{
  "success": true,
  "message": "WhatsApp conectado exitosamente",
  "service": "whatsapp",
  "metadata": {
    "phone_number": "+5491112345678"
  }
}
```

El backend valida las credenciales llamando a la API de WhatsApp antes de guardar.

**Paso 3: Desconectar**

```http
DELETE /integrations/whatsapp/disconnect
Authorization: Bearer {token}

Response:
{
  "success": true,
  "message": "WhatsApp desconectado exitosamente",
  "service": "whatsapp"
}
```

---

### 4. Telegram Bot

**Paso 1: Crear bot con @BotFather**

El usuario debe:
1. Abrir Telegram
2. Buscar `@BotFather`
3. Enviar `/newbot`
4. Seguir instrucciones
5. Copiar el token generado (ejemplo: `123456:ABC-DEF...`)

**Paso 2: Conectar**

```http
POST /integrations/telegram/connect
Authorization: Bearer {token}
Content-Type: application/json

{
  "bot_token": "123456789:ABCDEFGHijklmnopQRSTUVWXYZ-1234567890"
}

Response:
{
  "success": true,
  "message": "Telegram conectado exitosamente: @my_bot",
  "service": "telegram",
  "metadata": {
    "bot_username": "my_bot",
    "bot_name": "My Bot"
  }
}
```

El backend valida el token llamando a `getMe` de Telegram API antes de guardar.

**Paso 3: Desconectar**

```http
DELETE /integrations/telegram/disconnect
Authorization: Bearer {token}

Response:
{
  "success": true,
  "message": "Telegram desconectado exitosamente",
  "service": "telegram"
}
```

---

## Ejemplos de Uso

### Ejemplo 1: Frontend React - Conectar Gmail

```jsx
// Componente de integración
import { useState } from 'react';

function GmailConnect() {
  const [loading, setLoading] = useState(false);

  const connectGmail = async () => {
    setLoading(true);

    const token = localStorage.getItem('authToken');

    const response = await fetch('http://localhost:8000/integrations/gmail/connect', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });

    const data = await response.json();

    // Redirigir a Google OAuth
    window.location.href = data.authorization_url;
  };

  return (
    <button onClick={connectGmail} disabled={loading}>
      {loading ? 'Conectando...' : 'Conectar Gmail'}
    </button>
  );
}

// Página de callback (ej: /integrations)
function IntegrationsCallback() {
  const params = new URLSearchParams(window.location.search);
  const success = params.get('success');

  useEffect(() => {
    if (success === 'gmail') {
      toast.success('Gmail conectado exitosamente!');
    }
  }, [success]);

  return <div>Procesando conexión...</div>;
}
```

### Ejemplo 2: Frontend React - Conectar WhatsApp

```jsx
function WhatsAppConnect() {
  const [phoneNumberId, setPhoneNumberId] = useState('');
  const [accessToken, setAccessToken] = useState('');
  const [loading, setLoading] = useState(false);

  const connectWhatsApp = async () => {
    setLoading(true);

    const token = localStorage.getItem('authToken');

    const response = await fetch('http://localhost:8000/integrations/whatsapp/connect', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        phone_number_id: phoneNumberId,
        access_token: accessToken
      })
    });

    const data = await response.json();

    if (data.success) {
      toast.success('WhatsApp conectado exitosamente!');
    } else {
      toast.error('Error conectando WhatsApp');
    }

    setLoading(false);
  };

  return (
    <div>
      <input
        placeholder="Phone Number ID"
        value={phoneNumberId}
        onChange={(e) => setPhoneNumberId(e.target.value)}
      />
      <input
        placeholder="Access Token"
        type="password"
        value={accessToken}
        onChange={(e) => setAccessToken(e.target.value)}
      />
      <button onClick={connectWhatsApp} disabled={loading}>
        Conectar WhatsApp
      </button>
    </div>
  );
}
```

### Ejemplo 3: Listar Integraciones Conectadas

```jsx
function MyIntegrations() {
  const [integrations, setIntegrations] = useState({});

  useEffect(() => {
    const fetchIntegrations = async () => {
      const token = localStorage.getItem('authToken');

      const response = await fetch('http://localhost:8000/integrations/', {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });

      const data = await response.json();
      setIntegrations(data.integrations);
    };

    fetchIntegrations();
  }, []);

  const disconnect = async (service) => {
    const token = localStorage.getItem('authToken');

    await fetch(`http://localhost:8000/integrations/${service}/disconnect`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });

    // Refrescar lista
    window.location.reload();
  };

  return (
    <div>
      <h2>Mis Integraciones</h2>
      {Object.entries(integrations).map(([service, data]) => (
        <div key={service}>
          <h3>{service}</h3>
          <p>{data.metadata.email || data.metadata.bot_username}</p>
          <button onClick={() => disconnect(service)}>Desconectar</button>
        </div>
      ))}
    </div>
  );
}
```

---

## Seguridad y Almacenamiento

### Firestore Security Rules

**IMPORTANTE:** Configura reglas de seguridad en Firestore:

```javascript
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    // Usuarios solo pueden leer/escribir sus propias credenciales
    match /users/{userId}/external_credentials/{service} {
      allow read, write: if request.auth != null && request.auth.uid == userId;
    }
  }
}
```

### Encriptación

- ✅ Tokens se guardan en Firestore (ya encriptado en tránsito y reposo por Google)
- ✅ HTTPS obligatorio para OAuth callbacks
- ✅ JWT tokens con expiración de 24 horas
- ⚠️ **Recomendación adicional:** Implementar encriptación de campo para tokens sensibles

### Mejores Prácticas

1. **Rotar tokens regularmente**
   - Gmail: Refresh tokens automáticamente
   - Outlook: Refresh tokens automáticamente
   - WhatsApp/Telegram: Pedir al usuario que actualice

2. **Validar tokens antes de usar**
   - Verificar expiración
   - Regenerar si es necesario

3. **Logs y auditoría**
   - Registrar intentos de conexión
   - Alertar si hay muchos fallos

---

## Frontend Integration

### Flujo Completo de UX

```
1. Página de Integraciones
   ├── Mostrar servicios disponibles
   ├── Estado: Conectado / No conectado
   └── Botones: "Conectar" / "Desconectar"

2. Al hacer clic en "Conectar Gmail/Outlook"
   ├── POST /integrations/{service}/connect
   ├── Obtener authorization_url
   └── Redirigir a OAuth provider

3. Usuario autoriza en OAuth provider
   └── Redirige a /integrations/{service}/callback

4. Callback automático
   ├── Backend guarda credenciales
   └── Redirige a frontend con ?success={service}

5. Frontend muestra confirmación
   └── Toast: "Gmail conectado exitosamente!"

6. Para WhatsApp/Telegram (sin OAuth)
   ├── Mostrar formulario
   ├── Usuario ingresa credenciales
   ├── POST /integrations/{service}/connect con datos
   └── Mostrar confirmación
```

### Componente Reutilizable

```jsx
function IntegrationCard({ service, connected, metadata, onConnect, onDisconnect }) {
  const icons = {
    gmail: '📧',
    outlook: '📨',
    whatsapp: '💬',
    telegram: '✈️'
  };

  return (
    <div className="integration-card">
      <h3>{icons[service]} {service.toUpperCase()}</h3>

      {connected ? (
        <>
          <p>✅ Conectado</p>
          <p>{metadata?.email || metadata?.bot_username}</p>
          <button onClick={() => onDisconnect(service)}>
            Desconectar
          </button>
        </>
      ) : (
        <button onClick={() => onConnect(service)}>
          Conectar
        </button>
      )}
    </div>
  );
}
```

---

## Troubleshooting

### Gmail

**Error: "Redirect URI mismatch"**
```
Solución:
1. Ve a Google Cloud Console
2. Credentials > OAuth 2.0 Client ID
3. Agrega: http://localhost:8000/integrations/gmail/callback
4. Actualiza GMAIL_REDIRECT_URI en .env
```

**Error: "Token expired"**
```
Solución:
Gmail auto-refresca tokens. Si persiste:
1. Desconectar Gmail
2. Volver a conectar
```

### Outlook

**Error: "invalid_client"**
```
Solución:
1. Verifica OUTLOOK_CLIENT_ID y OUTLOOK_CLIENT_SECRET
2. Asegúrate de que el redirect URI esté registrado en Azure AD
```

### WhatsApp

**Error: "Credenciales inválidas"**
```
Posibles causas:
1. Token expirado (obtener nuevo desde Meta)
2. Phone Number ID incorrecto
3. Permisos insuficientes en Meta for Business

Solución:
1. Ve a https://business.facebook.com
2. Settings > API Setup
3. Genera nuevo token permanente
4. Vuelve a conectar
```

### Telegram

**Error: "Token inválido"**
```
Solución:
1. Verifica que copiaste el token completo de @BotFather
2. El token debe tener formato: 123456:ABC-DEF...
3. Si no funciona, crea un nuevo bot
```

### Firestore

**Error: "Permission denied"**
```
Solución:
1. Verifica que el usuario esté autenticado (JWT válido)
2. Revisa las reglas de seguridad de Firestore
3. Asegúrate de que el userId en el JWT coincida con el documento
```

---

## Próximos Pasos

### Features Planificadas

- [ ] Refresh automático de tokens OAuth
- [ ] Notificaciones cuando un token expira
- [ ] Soporte para múltiples cuentas del mismo servicio
- [ ] Webhooks personalizados por usuario
- [ ] Dashboard de uso de integraciones
- [ ] Logs de actividad por integración

### Mejoras de Seguridad

- [ ] Encriptación de campo para tokens
- [ ] 2FA para conexión de servicios
- [ ] Alertas de actividad sospechosa
- [ ] Rotación automática de tokens

---

## Recursos Adicionales

- [Google OAuth 2.0](https://developers.google.com/identity/protocols/oauth2)
- [Microsoft Graph API](https://learn.microsoft.com/en-us/graph/auth/)
- [WhatsApp Business API](https://developers.facebook.com/docs/whatsapp/cloud-api/get-started)
- [Telegram Bot API](https://core.telegram.org/bots/api)
- [Firestore Security Rules](https://firebase.google.com/docs/firestore/security/get-started)

---

**Actualizado:** 2025-10-31
**Versión:** 2.0.0
**Autor:** TransformAR Team
