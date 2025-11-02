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
from typing import Optional, Dict, Any

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

# Firestore structure
ORGANIZATION_COLLECTION = "organization"
USERS_SUBCOLLECTION = "users"
PASSWORD_RESETS_SUBCOLLECTION = "password_resets"
_organization_id = os.getenv("FIREBASE_ORGANIZATION_ID", "default")


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

def get_organization_id() -> str:
    """Return the configured organization identifier for Firestore."""
    global _organization_id
    if not _organization_id:
        _organization_id = "default"
    return _organization_id

def get_organization_ref(db=None, organization_id: Optional[str] = None):
    """Return the Firestore document reference for the current or specified organization."""
    if db is None:
        db = get_db()
    if db is None:
        return None
    org_id = organization_id
    if not org_id:
        return None
    return db.collection(ORGANIZATION_COLLECTION).document(org_id)

def get_users_collection(db=None, organization_id: Optional[str] = None):
    """Return the users subcollection reference within the organization."""
    org_ref = get_organization_ref(db, organization_id)
    if org_ref is None:
        return None
    return org_ref.collection(USERS_SUBCOLLECTION)

def get_user_document(user_id: str, db=None, organization_id: Optional[str] = None):
    """Return the document reference for a specific user within the organization."""
    users_ref = get_users_collection(db, organization_id)
    if users_ref is None:
        return None
    return users_ref.document(user_id)

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


async def authenticate_user(organization: str, email: str, password: str) -> Optional[Dict[str, Any]]:
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
        # Query users collection for the email within the organization
        print("organization authenticate: "+organization)
        print("email: "+ email)
        users_ref = get_users_collection(db, organization)
        if users_ref is None:
            return None
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
        print("ur id: " + user_doc.id)

        print("pass: "+ password)
        print("get pass: "+ user_data.get('password'))


        # Verify password
        if not password == user_data.get('password', ''):
            return None

        # Remove password from returned data
        user_data.pop('password', None)

        # Attach organization information for client convenience
        user_data['organization'] = organization

        return user_data

    except Exception as e:
        print(f"Error authenticating user: {e}")
        return None


async def get_user_by_email(email: str, organization: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
        # Query users collection for the email within the organization
        users_ref = get_users_collection(db, organization)
        if users_ref is None:
            return None
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


async def create_password_reset_token(email: str, organization: Optional[str] = None) -> Optional[str]:
    """
    Create a password reset token for a user

    Args:
        email: User's email address

    Returns:
        Reset token string if user exists, None otherwise
    """
    user = await get_user_by_email(email, organization)

    if not user:
        return None

    # Create a secure random token
    reset_token = secrets.token_urlsafe(32)

    # Store the reset token in Firestore with expiration
    db = get_db()
    if db is None:
        return None

    try:
        # Store in password_resets subcollection within the organization
        org_ref = get_organization_ref(db, organization)
        if org_ref is None:
            return None
        resets_ref = org_ref.collection(PASSWORD_RESETS_SUBCOLLECTION)
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
