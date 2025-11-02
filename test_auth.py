#!/usr/bin/env python3
"""
Script de prueba para verificar autenticación JWT
"""
import os
from auth import create_access_token

# Simular un usuario autenticado
test_user_data = {
    "sub": "test_user_123",  # user ID
    "email": "test@example.com"
}

# Crear token JWT
token = create_access_token(test_user_data)

print("=" * 60)
print("TOKEN JWT GENERADO:")
print("=" * 60)
print(token)
print()
print("=" * 60)
print("PRUEBA CON CURL:")
print("=" * 60)
print(f'curl -H "Authorization: Bearer {token}" http://localhost:8000/input/gmail/messages?limit=10')
print()
