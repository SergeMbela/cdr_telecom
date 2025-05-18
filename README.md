# CDR (Call Detail Records) Generator

Ce projet génère des enregistrements de données d'appels (CDR) simulés pour une base de données SQL Server.

## Fonctionnalités

- Génération de 10 millions d'enregistrements CDR
- Données réalistes pour la Belgique
- Gestion des interruptions et reprise automatique
- Notifications par email
- Logging détaillé
- Insertion par lots optimisée

## Prérequis

- Python 3.8+
- SQL Server
- ODBC Driver 17 for SQL Server

## Installation

1. Cloner le repository
2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

## Configuration

1. Créer un fichier `.env` à la racine du projet avec les variables suivantes :

```env
# Database Configuration
DB_SERVER=votre_serveur
DB_NAME=votre_base
DB_USER=votre_utilisateur
DB_PASSWORD=votre_mot_de_passe
DB_DRIVER={ODBC Driver 17 for SQL Server}
CDR_TABLE=nom_de_votre_table

# Email Configuration (Gmail)
GMAIL_USERNAME=votre_email@gmail.com
GMAIL_APP_PASSWORD=votre_mot_de_passe_application
RECIPIENT_EMAIL=email_destinataire@domaine.com
```

2. Pour Gmail, générer un mot de passe d'application :
   - Activer l'authentification à 2 facteurs
   - Aller dans Sécurité > Mots de passe des applications
   - Créer un nouveau mot de passe d'application

## Structure des données

Les CDR générés contiennent les champs suivants :
- ID (auto-incrémenté)
- Numéros d'appel (appelant/appelé)
- Date et heure de début
- Durée
- Type (voice/sms/data)
- Statut (completed/failed/missed)
- IMEI (appelant/appelé)
- Informations sur les antennes (4G/5G)
- Coordonnées géographiques
- Opérateurs

## Utilisation

```bash
python etl/generate_cdr.py
```

## Logs

Les logs sont stockés dans :
- Console
- Fichier `cdr_generation.log`

## Gestion des erreurs

- Reprise automatique après interruption
- 3 tentatives par lot en cas d'erreur
- Reconnexion automatique
- Notification par email en cas d'erreur

## Performance

- Insertion par lots de 10 000 enregistrements
- Indexation optimisée
- Gestion efficace de la mémoire

## Sécurité

- Variables d'environnement pour les informations sensibles
- Connexion chiffrée à la base de données
- Authentification sécurisée pour l'email

## Support

Pour toute question ou problème, veuillez créer une issue dans le repository. 