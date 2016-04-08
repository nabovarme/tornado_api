import tornado.ioloop
import tornado.web
import torndb

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.settings["db"]

class KwhLeftHandler(BaseHandler):
    def get(self):
        query = """
            SELECT meters.serial AS _serial,
                (SELECT ROUND(
                    (SELECT SUM(amount/price) AS paid_kwh FROM accounts WHERE serial = _serial) -
                    (SELECT
                        (SELECT samples.energy FROM samples WHERE samples.serial = _serial ORDER BY samples.unix_time DESC LIMIT 1) -
                        (SELECT meters.last_energy FROM meters WHERE meters.serial = _serial) AS consumed_kwh
                    ), 2) AS kwh_left) as kwh_diff
            FROM meters"""
        result_set = self.db.query(query)
        for obj in result_set:
            self.write(obj)

class MetersHandler(BaseHandler):
    def get(self):
        query = "SELECT 'serial', 'info', 'min_amount' FROM meters WHERE 'serial' IN (SELECT DISTINCT('serial') 'serial' FROM samples)"
        for meter in self.db.query(query):
            self.write(meter)

def make_app():
    db = torndb.Connection("10.0.1.9", "nabovarme", user="user", password="secret")
    return tornado.web.Application([
        (r"/meters", MetersHandler),
        (r"/kwh_left", KwhLeftHandler),
    ],db=db)

if __name__ == "__main__":
    app = make_app()
    app.listen(5000)
    tornado.ioloop.IOLoop.current().start()