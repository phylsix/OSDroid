from flask import Flask, redirect, session
from flask_compress import Compress

flask_app = Flask(__name__)

from .routes import main, tables, predhistory, docs

# Register the different blueprints
flask_app.register_blueprint(main)
flask_app.register_blueprint(tables)
flask_app.register_blueprint(predhistory)
flask_app.register_blueprint(docs)

Compress(flask_app)