from flask import Flask, redirect, session
from flask_compress import Compress

from .cache import cache

flask_app = Flask(__name__)
cache.init_app(flask_app)

from .routes import main, tables, predhistory, docs, issues

# Register the different blueprints
flask_app.register_blueprint(main)
flask_app.register_blueprint(tables)
flask_app.register_blueprint(predhistory)
flask_app.register_blueprint(docs)
flask_app.register_blueprint(issues)

Compress(flask_app)