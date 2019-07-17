from flask import Flask, redirect, session
from flask_compress import Compress

flask_app = Flask(__name__)

from .routes import main
from .views import tables, predhistory

# Register the different blueprints
flask_app.register_blueprint(main)
flask_app.register_blueprint(tables)
flask_app.register_blueprint(predhistory)

Compress(flask_app)