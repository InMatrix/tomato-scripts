from flask import Flask, request
from werkzeug import secure_filename
import time
import pdb
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO
from router_log_parser import RouterLogParser

app = Flask(__name__)
app.config.from_object('config_default')
try:
    app.config.from_envvar('LOG_RECEIVER_SETTINGS')
except RuntimeError:
    print "Starting application using default settings"


@app.route('/upload', methods=['PUT', 'POST'])
def upload_file():
    home_id = request.form['home_id']
    f = request.files['the_file']
    filename = f.filename
    timestamp = int(time.time())  # This is a UTC timestamp
    new_filename = secure_filename(filename + "_" + str(timestamp) + '.tsv')
    buf = StringIO(f.read())
    f.close()

    log_parser = RouterLogParser(home_id, app.config['DB_CONNECTION'])

    if filename == 'dnsmasq.leases':
        log_parser.update_dev_list(buf, timestamp)
        buf.seek(0)  # Return the cursor to the beginning of the file
    elif filename == 'webmon_recent_domains':
        log_parser.write_access_log(buf)
        buf.seek(0)
        log_parser.process_delayed_access_records()
    else:
        return "\n Unrecognized file received."

    # log_parser.archive_log(buf, new_filename)
    return "\n" + new_filename + "File is received and processed.\n"


@app.route('/', methods=['GET'])
def hello():
    return "hello"

if __name__ == '__main__':
    app.debug = app.config['DEBUG']
    app.run(host='0.0.0.0', port=app.config['PORT'])
