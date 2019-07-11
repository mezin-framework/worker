import traceback
import json
from time import sleep, time
from redis import Redis
from threading import Thread
from hashlib import sha256
from datetime import datetime
from random import randint
from utils.work_distributer.requester import RefreshRequester
from utils.services import plugin_service

current_milli_time = lambda: int(round(time() * 1000))

class RefreshWorker(object):

    def __init__(self):
        self.refresh_queue = RefreshQueue()
        self.metric = MetricPublisher()
        self.working = False
        Watcher().start()

    def run(self):
        while True:
            try:
                data = self.refresh_queue.get_new_payload()
                self.working = True
                raw_action = data.get('action')
                plugin = data.get('plugin')
                print plugin, raw_action
                action = plugin_service.get_worker_action_from_plugin(plugin, raw_action)

                start = current_milli_time()
                fetched_data = action(**data)
                duration = current_milli_time() - start

                if not fetched_data:
                    fetched_data = {}
                if not fetched_data.get('status'):
                    fetched_data['status'] = 'success'
                self.refresh_queue.respond(fetched_data)
                print duration
                self.metric.publish_metric({
                    "plugin": plugin,
                    "action": raw_action,
                    "duration": duration
                })
                self.working = False
            except:
                traceback.print_exc()
                self.refresh_queue.respond({"status": "internal_error"})
                self.working = False
                continue


class MetricPublisher(object):

    QUEUE = 'mezin:metrics'

    def __init__(self):
        self.redis = Redis(host='redis')

    def publish_metric(self, metric):
        self.redis.lpush(self.QUEUE, json.dumps(metric))


class RefreshQueue(object):
    ''' Responsible for encapsulating Queue behaviour,
        such as getting a new payload.
    '''

    QUEUE = 'worker_queue'

    def __init__(self):
        self.redis = Redis(host='redis')
        self.work_id = ''

    def get_new_payload(self):
        key, value = self.redis.brpop(self.QUEUE)
        data = json.loads(value.decode())
        self.work_id = data.get('work_id')
        self.redis.lpush(self.work_id, json.dumps({"status": "processing"}))
        return data

    def respond(self, data):
        if self.work_id:
            self.redis.lpush(self.work_id, json.dumps(data))
            self.work_id = ''


class Watcher(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.name = self.get_random_name()
        self.refresh_queue = RefreshQueue()
        self.refresh_queue.QUEUE = self.name
        self.requester = RefreshRequester('worker_registry')
        Thread(target=self.ask_register).start()

    def ask_register(self):
        while True:
            try:
                self.requester.block_request({"name": self.name, "action": "register"})
                sleep(60)
            except:
                continue

    def get_random_name(self):
        h = sha256()
        h.update(str(datetime.now()).encode('utf-8'))
        h.update(str(randint(0, 1000)))
        return h.hexdigest()

    def run(self):
        while True:
            try:
                data = self.refresh_queue.get_new_payload()
                action = data.get('action')
                if action == 'install_plugin':
                    Thread(target=plugin_service.install_plugin, args=(data.get('plugin_repo'),)).start()
                    self.refresh_queue.respond({'status': 'success'})
                elif action == 'ping':
                    self.refresh_queue.respond({"status": "ok"})
            except:
                traceback.print_exc()
                self.refresh_queue.respond({'status': 'error'})
