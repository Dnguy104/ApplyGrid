from flask import Flask
from ApplyGrid import app
app = Flask(__name__)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
