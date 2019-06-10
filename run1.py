from gevent.pywsgi import WSGIServer
from views import app

if __name__ == '__main__':
    http_server = WSGIServer(('0.0.0.0', 5000), app)
    http_server.serve_forever()

    # app.run(host='0.0.0.0', port=80, debug=True)
