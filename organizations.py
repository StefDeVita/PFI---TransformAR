"""
Módulo para gestionar organizaciones en Firestore.

Estructura en Firestore:
organizations/{orgId}
  - id: str
  - name: str
  - users: List[str]  # Lista de IDs de usuarios
  - templates: List[str]  # Lista de IDs de plantillas
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


async def create_organization(
    name: str,
    created_by_user_id: Optional[str] = None
) -> str:
    """
    Crea una nueva organización.

    Args:
        name: Nombre de la organización
        created_by_user_id: ID del usuario que crea la organización (opcional)

    Returns:
        ID de la organización creada
    """
    try:
        db = _get_db()
        org_id = str(uuid.uuid4())

        org_data = {
            "id": org_id,
            "name": name,
            "users": [created_by_user_id] if created_by_user_id else [],
            "templates": [],
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        doc_ref = db.collection("organizations").document(org_id)
        doc_ref.set(org_data)

        print(f"[Organizations] Organización creada: {org_id} - {name}")
        return org_id

    except Exception as e:
        print(f"[Organizations] Error creando organización: {e}")
        raise


async def get_organization(org_id: str) -> Optional[Dict[str, Any]]:
    """
    Obtiene los datos de una organización.

    Args:
        org_id: ID de la organización

    Returns:
        Diccionario con los datos de la organización o None si no existe
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)
        doc = doc_ref.get()

        if doc.exists:
            return doc.to_dict()
        return None

    except Exception as e:
        print(f"[Organizations] Error obteniendo organización {org_id}: {e}")
        return None


async def update_organization(
    org_id: str,
    name: Optional[str] = None
) -> bool:
    """
    Actualiza los datos de una organización.

    Args:
        org_id: ID de la organización
        name: Nuevo nombre (opcional)

    Returns:
        True si se actualizó correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)

        update_data = {
            "updated_at": firestore.SERVER_TIMESTAMP
        }

        if name is not None:
            update_data["name"] = name

        doc_ref.update(update_data)
        print(f"[Organizations] Organización actualizada: {org_id}")
        return True

    except Exception as e:
        print(f"[Organizations] Error actualizando organización {org_id}: {e}")
        return False


async def add_user_to_organization(org_id: str, user_id: str) -> bool:
    """
    Agrega un usuario a una organización.

    Args:
        org_id: ID de la organización
        user_id: ID del usuario a agregar

    Returns:
        True si se agregó correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)

        doc_ref.update({
            "users": firestore.ArrayUnion([user_id]),
            "updated_at": firestore.SERVER_TIMESTAMP
        })

        print(f"[Organizations] Usuario {user_id} agregado a organización {org_id}")
        return True

    except Exception as e:
        print(f"[Organizations] Error agregando usuario a organización: {e}")
        return False


async def remove_user_from_organization(org_id: str, user_id: str) -> bool:
    """
    Remueve un usuario de una organización.

    Args:
        org_id: ID de la organización
        user_id: ID del usuario a remover

    Returns:
        True si se removió correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)

        doc_ref.update({
            "users": firestore.ArrayRemove([user_id]),
            "updated_at": firestore.SERVER_TIMESTAMP
        })

        print(f"[Organizations] Usuario {user_id} removido de organización {org_id}")
        return True

    except Exception as e:
        print(f"[Organizations] Error removiendo usuario de organización: {e}")
        return False


async def add_template_to_organization(org_id: str, template_id: str) -> bool:
    """
    Asocia una plantilla a una organización.

    Args:
        org_id: ID de la organización
        template_id: ID de la plantilla

    Returns:
        True si se agregó correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)

        doc_ref.update({
            "templates": firestore.ArrayUnion([template_id]),
            "updated_at": firestore.SERVER_TIMESTAMP
        })

        print(f"[Organizations] Plantilla {template_id} agregada a organización {org_id}")
        return True

    except Exception as e:
        print(f"[Organizations] Error agregando plantilla a organización: {e}")
        return False


async def remove_template_from_organization(org_id: str, template_id: str) -> bool:
    """
    Remueve una plantilla de una organización.

    Args:
        org_id: ID de la organización
        template_id: ID de la plantilla

    Returns:
        True si se removió correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)

        doc_ref.update({
            "templates": firestore.ArrayRemove([template_id]),
            "updated_at": firestore.SERVER_TIMESTAMP
        })

        print(f"[Organizations] Plantilla {template_id} removida de organización {org_id}")
        return True

    except Exception as e:
        print(f"[Organizations] Error removiendo plantilla de organización: {e}")
        return False


async def get_organization_users(org_id: str) -> List[str]:
    """
    Obtiene la lista de IDs de usuarios de una organización.

    Args:
        org_id: ID de la organización

    Returns:
        Lista de IDs de usuarios
    """
    try:
        org = await get_organization(org_id)
        if org:
            return org.get("users", [])
        return []

    except Exception as e:
        print(f"[Organizations] Error obteniendo usuarios de organización: {e}")
        return []


async def get_organization_templates(org_id: str) -> List[str]:
    """
    Obtiene la lista de IDs de plantillas de una organización.

    Args:
        org_id: ID de la organización

    Returns:
        Lista de IDs de plantillas
    """
    try:
        org = await get_organization(org_id)
        if org:
            return org.get("templates", [])
        return []

    except Exception as e:
        print(f"[Organizations] Error obteniendo plantillas de organización: {e}")
        return []


async def list_organizations(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Lista todas las organizaciones.

    Args:
        limit: Número máximo de organizaciones a retornar

    Returns:
        Lista de organizaciones
    """
    try:
        db = _get_db()
        query = (
            db.collection("organizations")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(limit)
        )

        docs = query.stream()
        organizations = []

        for doc in docs:
            org_data = doc.to_dict()
            organizations.append(org_data)

        print(f"[Organizations] Obtenidas {len(organizations)} organizaciones")
        return organizations

    except Exception as e:
        print(f"[Organizations] Error listando organizaciones: {e}")
        return []


async def delete_organization(org_id: str) -> bool:
    """
    Elimina una organización.

    Args:
        org_id: ID de la organización a eliminar

    Returns:
        True si se eliminó correctamente
    """
    try:
        db = _get_db()
        doc_ref = db.collection("organizations").document(org_id)
        doc_ref.delete()

        print(f"[Organizations] Organización eliminada: {org_id}")
        return True

    except Exception as e:
        print(f"[Organizations] Error eliminando organización {org_id}: {e}")
        return False
