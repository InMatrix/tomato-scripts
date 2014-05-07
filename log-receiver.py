from flask import Flask, request
from werkzeug import secure_filename
import time, pdb
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO
import log_db as ld

app = Flask(__name__)

@app.route('/upload', methods=['PUT', 'POST'])
def upload_file():
	f = request.files['the_file']
	filename = f.filename
	timestamp = int(time.time())
	new_filename = secure_filename(filename + "_" + str(timestamp) + '.tsv')
	buf = StringIO(f.read())
	f.close()
	if filename == 'dnsmasq.leases':
		ld.update_dev_list(buf, timestamp)
		buf.seek(0) # Return the cursor to the beginning of the file
	elif filename == 'webmon_recent_domains':
		ld.write_access_log(buf)
		buf.seek(0)
		ld.process_delayed_access_records()
	else:
		return "\n Unrecognized file received."
	
	ld.archive_log(buf, new_filename)
	return "\n" + new_filename + "File is received and processed.\n"

@app.route('/', methods=['GET'])
def hello():
	return "hello"

if __name__ == '__main__':
	app.debug = True
	app.run(host='0.0.0.0')