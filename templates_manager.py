"""
Módulo para gestionar plantillas de transformación en Firestore.

Estructura en Firestore:
users/{userId}/templates/{templateId}
  - id: str
  - name: str
  - description: str
  - columns: List[Dict]  # GridColumn serializado
  - created_by: str  # userId
  - created_at: timestamp
  - updated_at: timestamp
"""

from __future__ import annotations
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
import firebase_admin
from firebase_admin import firestore


# Lazy initialization
def _get_db():
    """Obtiene la instancia de Firestore con lazy initialization"""
    from auth import get_db
    return get_db()


async def create_template(
    user_id: str,
    template_id: str,
    name: str,
    description: str,
    columns: List[Dict[str, Any]]
) -> bool:
    """
    Crea una nueva plantilla en Firestore.

    Args:
        user_id: ID del usuario que crea la plantilla
        template_id: ID único de la plantilla
        name: Nombre de la plantilla
        description: Descripción de la plantilla
        columns: Lista de columnas (GridColumn serializado)

    Returns:
        True si se creó correctamente
    """
    try:
        db = _get_db()

        template_data = {
            "id": template_id,
            "name": name,
            "description": description,
            "columns": columns,
            "created_by": user_id,
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        doc_ref = db.collection("users").document(user_id).collection("templates").document(template_id)
        doc_ref.set(template_data)

        print(f"[Templates] Plantilla creada: {template_id} por usuario {user_id}")
        return True

    except Exception as e:
        print(f"[Templates] Error creando plantilla: {e}")
        return False


async def get_template(user_id: str, template_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene una plantilla específica.

    Args:
        user_id: ID del usuario propietario
        template_id: ID de la plantilla

    Returns:
        Datos de la plantilla o None si no existe
    """
    try:
        db = _get_db()
        doc_ref = db.collection("users").document(user_id).collection("templates").document(template_id)
        doc = doc_ref.get()

        if doc.exists:
            return doc.to_dict()
        return None

    except Exception as e:
        print(f"[Templates] Error obteniendo plantilla {template_id}: {e}")
        return None


async def update_template(
    user_id: str,
    template_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    columns: Optional[List[Dict[str, Any]]] = None
) -> bool:
    """
    Actualiza una plantilla existente.

    Args:
        user_id: ID del usuario propietario
        template_id: ID de la plantilla
        name: Nuevo nombre (opcional)
        description: Nueva descripción (opcional)
        columns: Nuevas columnas (opcional)

    Returns:
        True si se actualizó correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("users").document(user_id).collection("templates").document(template_id)

        update_data = {
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        if name is not None:
            update_data["name"] = name
        if description is not None:
            update_data["description"] = description
        if columns is not None:
            update_data["columns"] = columns

        doc_ref.update(update_data)
        print(f"[Templates] Plantilla actualizada: {template_id}")
        return True

    except Exception as e:
        print(f"[Templates] Error actualizando plantilla {template_id}: {e}")
        return False


async def delete_template(user_id: str, template_id: str) -> bool:
    """
    Elimina una plantilla.

    Args:
        user_id: ID del usuario propietario
        template_id: ID de la plantilla

    Returns:
        True si se eliminó correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("users").document(user_id).collection("templates").document(template_id)
        doc_ref.delete()

        print(f"[Templates] Plantilla eliminada: {template_id}")
        return True

    except Exception as e:
        print(f"[Templates] Error eliminando plantilla {template_id}: {e}")
        return False


async def list_user_templates(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Lista todas las plantillas de un usuario.

    Args:
        user_id: ID del usuario
        limit: Número máximo de plantillas a retornar

    Returns:
        Lista de plantillas
    """
    try:
        db = _get_db()
        query = (
            db.collection("users")
            .document(user_id)
            .collection("templates")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(limit)
        )

        docs = query.stream()
        templates = []

        for doc in docs:
            template_data = doc.to_dict()
            templates.append(template_data)

        print(f"[Templates] Obtenidas {len(templates)} plantillas para usuario {user_id}")
        return templates

    except Exception as e:
        print(f"[Templates] Error listando plantillas: {e}")
        return []


async def get_template_metadata(user_id: str, template_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene solo los metadatos de una plantilla (sin las columnas).

    Args:
        user_id: ID del usuario propietario
        template_id: ID de la plantilla

    Returns:
        Metadatos de la plantilla (id, name, description)
    """
    try:
        template = await get_template(user_id, template_id)
        if template:
            return {
                "id": template.get("id"),
                "name": template.get("name"),
                "description": template.get("description", "")
            }
        return None

    except Exception as e:
        print(f"[Templates] Error obteniendo metadatos: {e}")
        return None


async def template_exists(user_id: str, template_id: str) -> bool:
    """
    Verifica si una plantilla existe.

    Args:
        user_id: ID del usuario propietario
        template_id: ID de la plantilla

    Returns:
        True si existe, False si no
    """
    try:
        db = _get_db()
        doc_ref = db.collection("users").document(user_id).collection("templates").document(template_id)
        doc = doc_ref.get()
        return doc.exists

    except Exception as e:
        print(f"[Templates] Error verificando existencia de plantilla: {e}")
        return False
