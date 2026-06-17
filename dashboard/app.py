"""
LUMBUNG — Flask Dashboard
Owner: Hanif

Membaca JSON export & model dari HDFS via pyarrow.fs.HadoopFileSystem.
TIDAK menyimpan data lokal — semua dari /data/lumbung/export/ di HDFS.
"""

from flask import Flask

app = Flask(__name__)


@app.route("/")
def index():
    return "LUMBUNG Dashboard — TODO"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
