from flask import Blueprint, render_template

baseline = Blueprint('baseline', __name__, template_folder ="templates",static_folder="static")
@baseline.route("/baseline")
def index():
    return render_template("/main/child.html")