from flask import Flask

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'f9ed5943a6c2e4fdd8f42ed4e17a5d0dfea33ef31f2b6ae933ee1634c707362d'

    # Import and register routes
    from .routes import main
    app.register_blueprint(main)

    return app
