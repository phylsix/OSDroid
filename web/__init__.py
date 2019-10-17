from flask import Flask, redirect, session
from flask_compress import Compress
from flask_wtf.csrf import CSRFProtect
from .cache import cache

flask_app = Flask(__name__)

csrf = CSRFProtect(flask_app)
import os
SECRET_KEY = os.urandom(32)
flask_app.config['SECRET_KEY'] = SECRET_KEY

cache.init_app(flask_app)

from .routes import main, tables, predhistory, docs, issues

# Register the different blueprints
flask_app.register_blueprint(main)
flask_app.register_blueprint(tables)
flask_app.register_blueprint(predhistory)
flask_app.register_blueprint(docs)
flask_app.register_blueprint(issues)

Compress(flask_app)