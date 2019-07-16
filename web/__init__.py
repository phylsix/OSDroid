from flask import Flask, redirect, session
from flask_compress import Compress

flask_app = Flask(__name__)

from .routes import main
from .views import tables

# Register the different blueprints
flask_app.register_blueprint(main)
flask_app.register_blueprint(tables)

Compress(flask_app)