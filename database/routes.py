from flask import Flask, Response, flash, redirect, render_template, request, url_for
import configparser
import csv
import io

import database


app = Flask(__name__)
app.secret_key = "replace-this-with-a-random-secret"
app.debug = True


def _load_flask_config():
    """
    Load basic Flask-related settings (currently only port) from config.ini.
    """
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config.get("FLASK", "port", fallback="5000")


@app.route("/")
def index():
    """
    Simple landing page that explains the tool and links to the export page.
    """
    page = {"title": "Database Export Tool"}
    return render_template("index.html", page=page)


@app.route("/export", methods=["GET", "POST"])
def export_table():
    """
    Allow the user to choose a table and export its contents with
    either a time range filter or an exact match on a column.
    The result is returned as a CSV download.
    """
    page = {"title": "Export table data"}

    # Always need a list of tables for the form
    try:
        tables = database.list_schema_tables()
    except Exception as exc:
        flash(f"Error fetching table list: {exc}")
        tables = []

    if request.method == "GET":
        return render_template("export.html", page=page, tables=tables)

    # POST: process export request
    table_name = request.form.get("table_name")
    filter_type = request.form.get("filter_type") or "none"
    attribute = request.form.get("attribute") or None
    start = request.form.get("start") or None
    end = request.form.get("end") or None
    exact = request.form.get("exact") or None

    try:
        cols, rows = database.export_table_raw(
            table_name=table_name,
            filter_type=filter_type,
            attribute=attribute,
            start=start,
            end=end,
            exact=exact,
        )
    except ValueError as e:
        # Validation / user errors -> show on the form again
        flash(str(e))
        return render_template("export.html", page=page, tables=tables)

    # Stream CSV back to the user
    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        # Header
        writer.writerow(cols)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        # Rows
        for row in rows:
            writer.writerow(row)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    filename = f"{table_name}_export.csv"
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# Convenience accessor for the port so web_app.py can use it
def get_port() -> int:
    return int(_load_flask_config())


