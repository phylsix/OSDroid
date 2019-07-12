import flask
from flask_compress import Compress

app = flask.Flask(__name__)
Compress(app)