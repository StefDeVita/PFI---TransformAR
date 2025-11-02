"""
Authentication module for handling user login and password recovery
Connects to Firestore for user management
"""

import os
import secrets
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any, List

import firebase_admin
from firebase_admin import credentials, firestore
from passlib.context import CryptContext
import jwt

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

# Firebase initialization
_firebase_initialized = False
_db = None


def initialize_firebase():
    """Initialize Firebase Admin SDK"""
    global _firebase_initialized, _db

    if _firebase_initialized:
        return _db

    try:
        # Check if Firebase is already initialized
        firebase_admin.get_app()
    except ValueError:
        # Firebase not initialized, initialize it
        cred_path = os.getenv("FIREBASE_CREDENTIALS_PATH", "firebase-credentials.json")

        if os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
        else:
            # Try to initialize with default credentials or application default credentials
            try:
                firebase_admin.initialize_app()
            except Exception as e:
                print(f"Warning: Firebase initialization failed: {e}")
                print("Please set FIREBASE_CREDENTIALS_PATH environment variable or place firebase-credentials.json in the root directory")
                return None

    _db = firestore.client()
    _firebase_initialized = True
    return _db


def get_db():
    """Get Firestore database instance"""
    if _db is None:
        return initialize_firebase()
    return _db


def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hashed password"""
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token

    Args:
        data: Dictionary containing the data to encode in the token
        expires_delta: Optional expiration time delta

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_jwt


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify and decode a JWT toke

    Args:
        token: JWT token string

    Returns:
        Decoded token data or None if invalid
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.PyJWTError:
        return None


def decode_jwt_token(token: str) -> Optional[str]:
    """
    Decode JWT token and extract user ID

    Args:
        token: JWT token string

    Returns:
        User ID or None if invalid
    """
    payload = verify_token(token)
    if payload:
        return payload.get("sub")  # "sub" contiene el user ID
    return None


async def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate a user with email and password

    Args:
        email: User's email address
        password: User's plain text password

    Returns:
        User data dictionary if authentication successful, None otherwise
    """
    db = get_db()

    if db is None:
        return None

    try:
        # Query users collection for the email
        users_ref = db.collection('users')
        query = users_ref.where('email', '==', email).limit(1)
        docs = query.stream()

        user_doc = None
        for doc in docs:
            user_doc = doc
            break

        if not user_doc:
            return None

        user_data = user_doc.to_dict()
        user_data['id'] = user_doc.id

        # Verify password
        if not password == user_data.get('password', ''):
            return None

        # Remove password from returned data
        user_data.pop('password', None)

        return user_data

    except Exception as e:
        print(f"Error authenticating user: {e}")
        return None


async def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Get user data by email

    Args:
        email: User's email address

    Returns:
        User data dictionary if found, None otherwise
    """
    db = get_db()

    if db is None:
        return None

    try:
        # Query users collection for the email
        users_ref = db.collection('users')
        query = users_ref.where('email', '==', email).limit(1)
        docs = query.stream()

        user_doc = None
        for doc in docs:
            user_doc = doc
            break

        if not user_doc:
            return None

        user_data = user_doc.to_dict()
        user_data['id'] = user_doc.id

        return user_data

    except Exception as e:
        print(f"Error getting user by email: {e}")
        return None


async def create_password_reset_token(email: str) -> Optional[str]:
    """
    Create a password reset token for a user

    Args:
        email: User's email address

    Returns:
        Reset token string if user exists, None otherwise
    """
    user = await get_user_by_email(email)

    if not user:
        return None

    # Create a secure random token
    reset_token = secrets.token_urlsafe(32)

    # Store the reset token in Firestore with expiration
    db = get_db()
    if db is None:
        return None

    try:
        # Store in password_resets collection
        resets_ref = db.collection('password_resets')
        resets_ref.add({
            'user_id': user['id'],
            'email': email,
            'token': reset_token,
            'created_at': firestore.SERVER_TIMESTAMP,
            'expires_at': datetime.utcnow() + timedelta(hours=1),  # 1 hour expiration
            'used': False
        })

        return reset_token

    except Exception as e:
        print(f"Error creating password reset token: {e}")
        return None


async def send_password_reset_email(email: str, reset_token: str) -> bool:
    """
    Send password reset email to user

    Args:
        email: User's email address
        reset_token: Password reset token

    Returns:
        True if email sent successfully, False otherwise
    """
    # Email configuration from environment variables
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    from_email = os.getenv("FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        print("Warning: SMTP credentials not configured. Email not sent.")
        print("Set SMTP_USER and SMTP_PASSWORD environment variables.")
        return False

    # Create reset URL (you may need to adjust this based on your frontend URL)
    reset_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
    reset_link = f"{reset_url}/reset-password?token={reset_token}"

    # Create email message
    message = MIMEMultipart("alternative")
    message["Subject"] = "Recuperación de Contraseña - TransformAR"
    message["From"] = from_email
    message["To"] = email

    # Create HTML content
    html = f"""
    <html>
      <body>
        <h2>Recuperación de Contraseña</h2>
        <p>Has solicitado recuperar tu contraseña.</p>
        <p>Haz clic en el siguiente enlace para restablecer tu contraseña:</p>
        <p><a href="{reset_link}">Restablecer Contraseña</a></p>
        <p>Este enlace expirará en 1 hora.</p>
        <p>Si no solicitaste este cambio, puedes ignorar este correo.</p>
        <br>
        <p>Saludos,<br>Equipo TransformAR</p>
      </body>
    </html>
    """

    # Create plain text version
    text = f"""
    Recuperación de Contraseña

    Has solicitado recuperar tu contraseña.

    Visita el siguiente enlace para restablecer tu contraseña:
    {reset_link}

    Este enlace expirará en 1 hora.

    Si no solicitaste este cambio, puedes ignorar este correo.

    Saludos,
    Equipo TransformAR
    """

    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    message.attach(part1)
    message.attach(part2)

    try:
        # Send email
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, email, message.as_string())

        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False


# ============================================================================
# User Management Functions (con organizaciones)
# ============================================================================

async def create_user(
    email: str,
    password: str,
    name: str,
    organization_id: str
) -> Optional[Dict[str, Any]]:
    """
    Crea un nuevo usuario en la colección users.

    Args:
        email: Correo electrónico del usuario
        password: Contraseña en texto plano
        name: Nombre completo del usuario
        organization_id: ID de la organización a la que pertenece

    Returns:
        Datos del usuario creado o None si falla
    """
    db = get_db()

    if db is None:
        return None

    try:
        # Verificar que el email no exista
        existing_user = await get_user_by_email(email)
        if existing_user:
            print(f"[Auth] Usuario con email {email} ya existe")
            return None

        # Crear documento de usuario
        users_ref = db.collection('users')
        user_doc_ref = users_ref.document()  # Genera ID automático

        user_data = {
            "id": user_doc_ref.id,
            "name": name,
            "email": email,
            "password": password,  # En producción, considerar hashear
            "organization": organization_id,  # Referencia a organización
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        user_doc_ref.set(user_data)

        print(f"[Auth] Usuario creado: {user_doc_ref.id} - {email}")

        # Agregar usuario a la organización
        from organizations import add_user_to_organization
        await add_user_to_organization(organization_id, user_doc_ref.id)

        # Eliminar password antes de retornar
        user_data_response = user_data.copy()
        user_data_response.pop('password', None)

        return user_data_response

    except Exception as e:
        print(f"[Auth] Error creando usuario: {e}")
        return None


async def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene datos de un usuario por su ID.

    Args:
        user_id: ID del usuario

    Returns:
        Datos del usuario o None si no existe
    """
    db = get_db()

    if db is None:
        return None

    try:
        user_doc = db.collection('users').document(user_id).get()

        if not user_doc.exists:
            return None

        user_data = user_doc.to_dict()
        user_data['id'] = user_doc.id

        # Eliminar password
        user_data.pop('password', None)

        return user_data

    except Exception as e:
        print(f"[Auth] Error obteniendo usuario {user_id}: {e}")
        return None


async def update_user(
    user_id: str,
    name: Optional[str] = None,
    email: Optional[str] = None,
    organization_id: Optional[str] = None
) -> bool:
    """
    Actualiza datos de un usuario.

    Args:
        user_id: ID del usuario
        name: Nuevo nombre (opcional)
        email: Nuevo email (opcional)
        organization_id: Nueva organización (opcional)

    Returns:
        True si se actualizó correctamente
    """
    db = get_db()

    if db is None:
        return False

    try:
        user_ref = db.collection('users').document(user_id)

        update_data = {
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        if name is not None:
            update_data["name"] = name
        if email is not None:
            update_data["email"] = email
        if organization_id is not None:
            update_data["organization"] = organization_id

        user_ref.update(update_data)
        print(f"[Auth] Usuario actualizado: {user_id}")
        return True

    except Exception as e:
        print(f"[Auth] Error actualizando usuario {user_id}: {e}")
        return False


async def get_users_by_organization(organization_id: str) -> List[Dict[str, Any]]:
    """
    Obtiene todos los usuarios de una organización.

    Args:
        organization_id: ID de la organización

    Returns:
        Lista de usuarios
    """
    db = get_db()

    if db is None:
        return []

    try:
        users_ref = db.collection('users')
        query = users_ref.where('organization', '==', organization_id)
        docs = query.stream()

        users = []
        for doc in docs:
            user_data = doc.to_dict()
            user_data['id'] = doc.id
            user_data.pop('password', None)
            users.append(user_data)

        print(f"[Auth] Obtenidos {len(users)} usuarios de organización {organization_id}")
        return users

    except Exception as e:
        print(f"[Auth] Error obteniendo usuarios de organización: {e}")
        return []
