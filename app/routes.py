from flask import Blueprint, render_template, request, jsonify
from .query_handler import NLMDQA

main = Blueprint('main', __name__)

# Initialize the NLMDQA system
nlmdqa = NLMDQA()

@main.route('/')
def index():
    return render_template('index.html')  # Renders your HTML file

@main.route('/api/query', methods=['POST'])
async def handle_query():
    data = request.json
    query = data.get('query')

    if not query:
        return jsonify({"error": "Query cannot be empty"}), 400

    try:
        results = await nlmdqa.process_query(query, human_readable=True)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
