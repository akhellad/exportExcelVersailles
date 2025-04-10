from flask import Flask, request, send_file, jsonify
import os
import tempfile
from export import export_tournee_vers_excel
import logging
from werkzeug.middleware.proxy_fix import ProxyFix

# Configuration du logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Support pour les proxys (utile lors de l'hébergement)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Répertoire pour stocker temporairement les fichiers Excel générés
TEMP_DIR = tempfile.gettempdir()

@app.route('/')
def index():
    """Page d'accueil simple avec des informations sur l'API"""
    return """
    <html>
        <head>
            <title>API d'exportation des tournées</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                h1 { color: #333; }
                code { background-color: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
            </style>
        </head>
        <body>
            <h1>API d'exportation des tournées</h1>
            <p>Cette API permet d'exporter les données d'une tournée vers un fichier Excel.</p>
            <h2>Utilisation</h2>
            <ul>
                <li><strong>GET /export-tournee?id=123</strong> - Exporte la tournée avec l'ID spécifié</li>
                <li><strong>POST /export-tournee</strong> - Exporte la tournée en envoyant l'ID dans le corps de la requête (JSON)</li>
            </ul>
        </body>
    </html>
    """

@app.route('/export-tournee', methods=['GET', 'POST'])
def export_tournee():
    """
    Endpoint pour exporter une tournée vers Excel.
    Accepte l'ID de la tournée soit par paramètre GET soit par POST JSON.
    """
    tournee_id = None
    
    if request.method == 'GET':
        tournee_id = request.args.get('id')
    elif request.method == 'POST':
        if request.is_json:
            data = request.get_json()
            tournee_id = data.get('id')
        else:
            # Tenter de récupérer les données de formulaire
            tournee_id = request.form.get('id')
    
    # Vérifier que l'ID de tournée est fourni
    if not tournee_id:
        return jsonify({"error": "Veuillez fournir un ID de tournée"}), 400
    
    logger.info(f"Demande d'exportation pour la tournée ID: {tournee_id}")
    
    try:
        # Générer un nom de fichier unique pour cette exportation
        output_file = os.path.join(TEMP_DIR, f"tournee_{tournee_id}_{os.urandom(4).hex()}.xlsx")
        
        # Appeler la fonction d'exportation
        success = export_tournee_vers_excel(tournee_id=tournee_id, output_file=output_file)
        
        if success and os.path.exists(output_file):
            logger.info(f"Exportation réussie. Fichier généré: {output_file}")
            
            # Renvoyer le fichier Excel au client
            return send_file(
                output_file,
                as_attachment=True,
                download_name=f"Tournee_{tournee_id}.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            logger.error(f"Échec de l'exportation pour la tournée ID: {tournee_id}")
            return jsonify({"error": "Échec de l'exportation. Vérifiez les logs du serveur."}), 500
    
    except Exception as e:
        logger.exception(f"Erreur lors de l'exportation: {str(e)}")
        return jsonify({"error": f"Erreur lors de l'exportation: {str(e)}"}), 500

@app.route('/health')
def health_check():
    """Endpoint pour vérifier que l'API est en ligne"""
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    # Pour le développement local, utilisez debug=True
    # Pour la production, définissez debug=False
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)