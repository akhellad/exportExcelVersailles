from PyConnectDataverse import authenticate_with_msal
import sys
import json
import pandas as pd
import os

class NewDataverseConnector:
    """Gère la connexion et les requêtes à Dataverse pour la nouvelle application"""
    def __init__(self, client_id, tenant_id, env_url, path_to_env=None):
        """
        Initialise un connecteur pour Dataverse.
        
        Args:
            client_id (str): L'ID client de l'application dans Azure AD
            tenant_id (str): L'ID du tenant Azure AD
            env_url (str): L'URL de l'environnement Dataverse
            path_to_env (str, optional): Chemin vers le fichier de configuration d'environnement
        """
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.env_url = env_url
        
        # Créer un fichier d'environnement par défaut si nécessaire
        if path_to_env is None:
            current_dir = os.getcwd()
            self.path_to_env = f"{current_dir}/PyConnectDataverse/new-app-env.json"
        else:
            self.path_to_env = path_to_env
            
        # Vérifier que le dossier PyConnectDataverse existe
        pyconnect_dir = os.path.dirname(self.path_to_env)
        if not os.path.exists(pyconnect_dir):
            os.makedirs(pyconnect_dir, exist_ok=True)
            print(f"Dossier créé: {pyconnect_dir}")
        
        # Créer le fichier d'environnement s'il n'existe pas
        if not os.path.exists(self.path_to_env):
            try:
                with open(self.path_to_env, 'w') as f:
                    # Utiliser la structure exacte qui fonctionne pour votre app précédente
                    json.dump({
                        "clientID": self.client_id,
                        "tenantID": self.tenant_id,
                        "environmentURI": self.env_url,
                        "authorityBase": "https://login.microsoftonline.com/",
                        "scopeSuffix": "user_impersonation"
                    }, f, indent=4)
                print(f"Fichier d'environnement créé: {self.path_to_env}")
            except Exception as e:
                print(f"Erreur lors de la création du fichier d'environnement: {str(e)}")
        
        self.session_token = None
        self.env_token = None

    def get_access_token(self, path):
        """Obtient le token d'accès"""
        try:
            authentication = authenticate_with_msal.getAuthenticatedSession(path)
            return authentication[0], authentication[1]
        except Exception as e:
            print(f"Erreur lors de l'obtention du token: {str(e)}")
            raise

    def connect(self):
        """Établit la connexion avec Dataverse"""
        try:
            self.session_token, self.env_token = self.get_access_token(self.path_to_env)
            print("Connexion à Dataverse réussie")
            return True
        except Exception as e:
            print(f"Erreur de connexion à Dataverse: {str(e)}")
            return False

    def get_entity_set_name(self, logical_name):
        """
        Récupère le nom de l'ensemble d'entités (utilisé dans l'URL de l'API) 
        à partir du nom logique d'une table.
        
        Args:
            logical_name (str): Nom logique de la table
            
        Returns:
            str: Nom de l'ensemble d'entités, ou None si non trouvé
        """
        request_uri = f'{self.env_token}api/data/v9.2/EntityDefinitions(LogicalName=\'{logical_name}\')?$select=EntitySetName'
        
        try:
            r = self.session_token.get(request_uri)
            
            if r.status_code != 200:
                print(f"Requête échouée pour EntitySetName de {logical_name}: Code {r.status_code}")
                return None
                
            data = json.loads(r.content.decode('utf-8'))
            if 'EntitySetName' in data:
                entity_set_name = data['EntitySetName']
                print(f"Nom d'entité pour l'API: {entity_set_name}")
                return entity_set_name
            else:
                print(f"Pas de EntitySetName trouvé pour {logical_name}")
                return None
                
        except Exception as e:
            print(f"Erreur lors de la récupération du nom d'entité: {str(e)}")
            return None

    def get_table_data(self, table_name, select=None, filter=None, only_custom=True):
        """
        Récupère les données d'une table Dataverse avec options de filtrage.
        
        Args:
            table_name (str): Nom logique de la table Dataverse
            select (str, optional): Colonnes à sélectionner, séparées par des virgules
            filter (str, optional): Filtre OData à appliquer
            only_custom (bool, optional): Si True, ne retourne que les colonnes personnalisées (crcfe_ ou new_)
            
        Returns:
            pandas.DataFrame: DataFrame contenant les données de la table
        """
        # Obtenir le nom correct pour l'API
        entity_set_name = self.get_entity_set_name(table_name)
        if not entity_set_name:
            # Si on ne peut pas obtenir le nom correct, essayer avec le nom original
            entity_set_name = table_name
        
        # Construire l'URL de requête
        request_uri = f'{self.env_token}api/data/v9.2/{entity_set_name}'
        
        # D'abord récupérer toutes les données sans sélection de colonnes spécifiques
        # pour éviter les problèmes avec les colonnes Lookup
        query_options = []
        if filter:
            query_options.append(f'$filter={filter}')
            
        if query_options:
            request_uri += '?' + '&'.join(query_options)
        
        print(f"URL de requête: {request_uri}")
            
        # Exécuter la requête
        try:
            r = self.session_token.get(request_uri)
            
            if r.status_code != 200:
                print(f"Requête échouée pour la table {table_name}: Code {r.status_code}")
                if r.text:
                    print(f"Détails de l'erreur: {r.text[:200]}...")
                return None
                
            raw = json.loads(r.content.decode('utf-8'))
            if 'value' not in raw:
                print(f"La clé 'value' n'est pas dans la réponse pour la table {table_name}")
                return None
                
            df = pd.DataFrame(raw['value'])
            
            if df.empty:
                print(f"Aucune donnée trouvée dans la table {table_name}")
                return df
            
            # Filtrer pour ne conserver que les colonnes personnalisées si demandé
            if only_custom and not df.empty:
                # Trouver toutes les colonnes qui commencent par crcfe_ ou new_
                custom_cols = [col for col in df.columns if col.startswith('crcfe_') or col.startswith('new_')]
                
                # S'assurer qu'on a des colonnes à afficher
                if custom_cols:
                    print(f"Colonnes personnalisées extraites: {len(custom_cols)}")
                    return df[custom_cols]
                else:
                    print("Aucune colonne personnalisée trouvée dans les données")
                    return pd.DataFrame()
            
            return df
            
        except Exception as e:
            print(f"Erreur lors de la récupération des données: {str(e)}")
            return None
            
    def list_tables(self):
        """
        Liste les tables disponibles dans l'environnement Dataverse 
        commençant par "new_" ou "crcfe_".
        
        Returns:
            pandas.DataFrame: DataFrame contenant les informations des tables filtrées
        """
        request_uri = f'{self.env_token}api/data/v9.2/EntityDefinitions?$select=LogicalName'
        
        try:
            r = self.session_token.get(request_uri)
            
            if r.status_code != 200:
                print(f"Requête échouée pour la liste des tables: Code {r.status_code}")
                return None
                
            raw = json.loads(r.content.decode('utf-8'))
            if 'value' not in raw:
                print(f"La clé 'value' n'est pas dans la réponse")
                return None
                
            df = pd.DataFrame(raw['value'])
            print(f"Nombre total de tables trouvées: {len(df)}")
            
            # Filtrer pour ne garder que les tables commençant par "new_" ou "crcfe_"
            if 'LogicalName' in df.columns:
                filtered_df = df[
                    df['LogicalName'].str.startswith('new_') | 
                    df['LogicalName'].str.startswith('crcfe_')
                ]
                print(f"Nombre de tables après filtrage (new_ ou crcfe_): {len(filtered_df)}")
                return filtered_df[['LogicalName']]
            else:
                return df
            
        except Exception as e:
            print(f"Erreur lors de la récupération des tables: {str(e)}")
            return None
    
    def list_columns(self, table_name):
        """
        Liste les colonnes personnalisées d'une table spécifique 
        (celles commençant par "crcfe_" ou "new_").
        
        Args:
            table_name (str): Nom logique de la table
                
        Returns:
            pandas.DataFrame: DataFrame contenant les informations des colonnes
        """
        request_uri = (f'{self.env_token}api/data/v9.2/EntityDefinitions(LogicalName=\'{table_name}\')'
                    f'/Attributes?$select=LogicalName,AttributeType')
        
        try:
            r = self.session_token.get(request_uri)
            
            if r.status_code != 200:
                print(f"Requête échouée pour les colonnes de {table_name}: Code {r.status_code}")
                return None
                    
            raw = json.loads(r.content.decode('utf-8'))
            if 'value' not in raw:
                print(f"La clé 'value' n'est pas dans la réponse")
                return None
                    
            df = pd.DataFrame(raw['value'])
            print(f"Nombre total de colonnes trouvées: {len(df)}")
            
            # Filtrer pour ne garder que les colonnes personnalisées
            if 'LogicalName' in df.columns:
                filtered_df = df[
                    df['LogicalName'].str.startswith('crcfe_') | 
                    df['LogicalName'].str.startswith('new_')
                ]
                print(f"Nombre de colonnes personnalisées (crcfe_ ou new_): {len(filtered_df)}")
                
                if 'AttributeType' in filtered_df.columns:
                    return filtered_df[['LogicalName', 'AttributeType']]
                else:
                    return filtered_df[['LogicalName']]
            else:
                return df
                
        except Exception as e:
            print(f"Erreur lors de la récupération des colonnes: {str(e)}")
            return None