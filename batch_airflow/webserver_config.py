#
# Apache License 2.0
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.
#
# This configuration file defines default settings for the
# Apache Airflow Webserver (Flask AppBuilder based UI).
#
# It controls:
# - Authentication type
# - Security settings (CSRF)
# - User registration options
# - OAuth / LDAP / OpenID configuration
# - UI theme customization
#
# NOTE:
# This file contains configuration defaults.
# Modify only the required parameters for your deployment.
#

from __future__ import annotations

import os

from flask_appbuilder.const import AUTH_DB

# Optional authentication imports (uncomment if needed)
# from airflow.www.fab_security.manager import AUTH_LDAP
# from airflow.www.fab_security.manager import AUTH_OAUTH
# from airflow.www.fab_security.manager import AUTH_OID
# from airflow.www.fab_security.manager import AUTH_REMOTE_USER


# Base directory reference
basedir = os.path.abspath(os.path.dirname(__file__))

# ----------------------------------------------------
# CSRF SECURITY CONFIGURATION
# ----------------------------------------------------
# Enables protection against Cross-Site Request Forgery attacks.
WTF_CSRF_ENABLED = True

# CSRF token expiration time (None = no expiration)
WTF_CSRF_TIME_LIMIT = None


# ----------------------------------------------------
# AUTHENTICATION CONFIGURATION
# ----------------------------------------------------
# Flask AppBuilder supports multiple authentication methods.
# Official documentation:
# https://flask-appbuilder.readthedocs.io/en/latest/security.html#authentication-methods

# Available authentication types:
# AUTH_OID         → OpenID
# AUTH_DB          → Database (default Airflow authentication)
# AUTH_LDAP        → LDAP server
# AUTH_REMOTE_USER → Web server provided authentication
# AUTH_OAUTH       → OAuth providers (Google, GitHub, etc.)

# Default authentication type (Database authentication)
AUTH_TYPE = AUTH_DB


# ----------------------------------------------------
# ROLE CONFIGURATION (Optional)
# ----------------------------------------------------

# Define custom Admin role name (optional)
# AUTH_ROLE_ADMIN = 'Admin'

# Allow public (unauthenticated) access by assigning a default role
# Example: AUTH_ROLE_PUBLIC = 'Viewer'
# AUTH_ROLE_PUBLIC = 'Viewer'


# ----------------------------------------------------
# USER SELF-REGISTRATION SETTINGS (Optional)
# ----------------------------------------------------

# Enable user self-registration
# AUTH_USER_REGISTRATION = True

# If self-registration is enabled, reCAPTCHA keys are required
# RECAPTCHA_PRIVATE_KEY = "YOUR_PRIVATE_KEY"
# RECAPTCHA_PUBLIC_KEY = "YOUR_PUBLIC_KEY"

# Email configuration for registration confirmation
# MAIL_SERVER = 'smtp.gmail.com'
# MAIL_USE_TLS = True
# MAIL_USERNAME = 'yourappemail@gmail.com'
# MAIL_PASSWORD = 'your_email_password'
# MAIL_DEFAULT_SENDER = 'sender@gmail.com'

# Default role assigned to newly registered users
# AUTH_USER_REGISTRATION_ROLE = "Public"


# ----------------------------------------------------
# OAUTH CONFIGURATION (Optional)
# ----------------------------------------------------
# Example configuration for Google OAuth.
# Uncomment and configure with your credentials if needed.

# OAUTH_PROVIDERS = [{
#     'name': 'google',
#     'token_key': 'access_token',
#     'icon': 'fa-google',
#     'remote_app': {
#         'api_base_url': 'https://www.googleapis.com/oauth2/v2/',
#         'client_kwargs': {
#             'scope': 'email profile'
#         },
#         'access_token_url': 'https://accounts.google.com/o/oauth2/token',
#         'authorize_url': 'https://accounts.google.com/o/oauth2/auth',
#         'request_token_url': None,
#         'client_id': GOOGLE_KEY,
#         'client_secret': GOOGLE_SECRET_KEY,
#     }
# }]


# ----------------------------------------------------
# LDAP CONFIGURATION (Optional)
# ----------------------------------------------------

# Example LDAP server configuration
# AUTH_LDAP_SERVER = "ldap://your-ldap-server"


# ----------------------------------------------------
# OPENID CONFIGURATION (Optional)
# ----------------------------------------------------

# Example OpenID providers
# OPENID_PROVIDERS = [
#     { 'name': 'Yahoo', 'url': 'https://me.yahoo.com' },
#     { 'name': 'AOL', 'url': 'http://openid.aol.com/<username>' },
#     { 'name': 'Flickr', 'url': 'http://www.flickr.com/<username>' },
#     { 'name': 'MyOpenID', 'url': 'https://www.myopenid.com' }
# ]


# ----------------------------------------------------
# UI THEME CONFIGURATION
# ----------------------------------------------------
# Airflow (Flask AppBuilder) supports multiple UI themes.
# Documentation:
# https://flask-appbuilder.readthedocs.io/en/latest/customizing.html#changing-themes
#
# IMPORTANT:
# Remove "navbar_color" from airflow.cfg
# to fully apply selected theme styles.

# Available themes:
# APP_THEME = "bootstrap-theme.css"  # Default Bootstrap
# APP_THEME = "amelia.css"
# APP_THEME = "cerulean.css"
# APP_THEME = "cosmo.css"
# APP_THEME = "cyborg.css"
# APP_THEME = "darkly.css"
# APP_THEME = "flatly.css"
# APP_THEME = "journal.css"
# APP_THEME = "lumen.css"
# APP_THEME = "paper.css"
# APP_THEME = "readable.css"
# APP_THEME = "sandstone.css"
# APP_THEME = "simplex.css"
# APP_THEME = "slate.css"
# APP_THEME = "solar.css"
# APP_THEME = "spacelab.css"
# APP_THEME = "superhero.css"
# APP_THEME = "united.css"
# APP_THEME = "yeti.css"
